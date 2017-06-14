package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"time"

	. "github.com/claudetech/loggo/default"
	"github.com/orcaman/concurrent-map"
)

// DownloadManager handles concurrent chunk downloads
type DownloadManager struct {
	Client        *http.Client
	ChunkManager  *ChunkManager
	ReadAhead     int
	HighPrioQueue chan *downloadRequest
	LowPrioQueue  chan *downloadRequest
	DownloadQueue cmap.ConcurrentMap
}

type downloadRequest struct {
	object   *APIObject
	offset   int64
	size     int64
	response chan *downloadResponse
}

type downloadResponse struct {
	content []byte
	err     error
}

// NewDownloadManager creates a new download manager
func NewDownloadManager(
	threadCount,
	chunkReadAhead int,
	client *http.Client,
	chunkManager *ChunkManager) (*DownloadManager, error) {

	manager := DownloadManager{
		Client:        client,
		ChunkManager:  chunkManager,
		ReadAhead:     chunkReadAhead,
		HighPrioQueue: make(chan *downloadRequest),
		LowPrioQueue:  make(chan *downloadRequest),
		DownloadQueue: cmap.New(),
	}

	if threadCount < 1 {
		return nil, fmt.Errorf("Number of threads for download manager must not be < 1")
	}

	// TODO: multiple threads
	// for i := 0; i < threadCount; i++ {
	// 	go manager.downloadThread()
	// }
	go manager.downloadThreadHighPrio()
	go manager.downloadThreadLowPrio()

	return &manager, nil
}

// DownloadHighPrio downloads a chunk with high priority
func (m *DownloadManager) DownloadHighPrio(object *APIObject, offset, size int64) ([]byte, error) {
	responseChannel := make(chan *downloadResponse)
	m.HighPrioQueue <- &downloadRequest{
		object:   object,
		offset:   offset,
		size:     size,
		response: responseChannel,
	}

	go func() {
		readAheadOffset := offset + m.ChunkManager.ChunkSize
		for i := 0; i < m.ReadAhead && uint64(readAheadOffset) < object.Size; i++ {
			m.LowPrioQueue <- &downloadRequest{
				object: object,
				offset: readAheadOffset,
				size:   size,
			}
			readAheadOffset += m.ChunkManager.ChunkSize
		}
	}()

	response := <-responseChannel

	if nil != response.err {
		return nil, response.err
	}
	return response.content, nil
}

func (m *DownloadManager) downloadThreadHighPrio() {
	for {
		m.getChunk(<-m.HighPrioQueue, true)
	}
}

func (m *DownloadManager) downloadThreadLowPrio() {
	for {
		m.getChunk(<-m.LowPrioQueue, false)
	}
}

func (m *DownloadManager) getChunk(request *downloadRequest, highPrio bool) {
	fOffset := request.offset % m.ChunkManager.ChunkSize
	offsetStart := request.offset - fOffset
	chunkID := fmt.Sprintf("%v:%v", request.object.ObjectID, offsetStart)

	bytes, err := m.ChunkManager.GetChunk(request.object, request.offset, request.size)
	if nil == err {
		if nil != request.response {
			request.response <- &downloadResponse{
				content: bytes,
			}
		}
		return
	}
	Log.Tracef("%v", err)

	// check if chunk is already downloading and wait for it
	if m.DownloadQueue.Has(chunkID) {
		Log.Debugf("Chunk %v already downloading, waiting...", chunkID)
		time.Sleep(100 * time.Millisecond)
		m.getChunk(request, highPrio)
		return
	}

	m.DownloadQueue.SetIfAbsent(chunkID, true)
	bytes, err = downloadFromAPI(m.Client, m.ChunkManager.ChunkSize, 0, request, highPrio)
	if nil != err {
		if nil != request.response {
			request.response <- &downloadResponse{
				err: err,
			}
		}
	}

	m.ChunkManager.StoreChunk(request.object, request.offset, bytes)
	m.DownloadQueue.Remove(chunkID)

	sOffset := int64(math.Min(float64(fOffset), float64(len(bytes))))
	eOffset := int64(math.Min(float64(fOffset+request.size), float64(len(bytes))))

	if nil != request.response {
		request.response <- &downloadResponse{
			content: bytes[sOffset:eOffset],
		}
	}
}

func downloadFromAPI(client *http.Client, chunkSize, delay int64, request *downloadRequest, highPrio bool) ([]byte, error) {
	// sleep if request is throttled
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	fOffset := request.offset % chunkSize
	offsetStart := request.offset - fOffset
	offsetEnd := offsetStart + chunkSize

	Log.Debugf("Requesting object %v (%v) bytes %v - %v from API (high priority: %v)",
		request.object.ObjectID, request.object.Name, offsetStart, offsetEnd, highPrio)
	req, err := http.NewRequest("GET", request.object.DownloadURL, nil)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not create request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%v-%v", offsetStart, offsetEnd))

	Log.Tracef("Sending HTTP Request %v", req)

	res, err := client.Do(req)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}
	defer res.Body.Close()
	reader := res.Body

	if res.StatusCode != 206 {
		if res.StatusCode != 403 {
			Log.Debugf("Request\n----------\n%v\n----------\n", req)
			Log.Debugf("Response\n----------\n%v\n----------\n", res)
			return nil, fmt.Errorf("Wrong status code %v", res.StatusCode)
		}

		// throttle requests
		if delay > 8 {
			return nil, fmt.Errorf("Maximum throttle interval has been reached")
		}
		bytes, err := ioutil.ReadAll(reader)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not read body of 403 error")
		}
		body := string(bytes)
		if strings.Contains(body, "dailyLimitExceeded") ||
			strings.Contains(body, "userRateLimitExceeded") ||
			strings.Contains(body, "rateLimitExceeded") ||
			strings.Contains(body, "backendError") {
			if 0 == delay {
				delay = 1
			} else {
				delay = delay * 2
			}
			return downloadFromAPI(client, chunkSize, delay, request, highPrio)
		}

		// return an error if other 403 error occurred
		Log.Debugf("%v", body)
		return nil, fmt.Errorf("Could not read object %v (%v) / StatusCode: %v",
			request.object.ObjectID, request.object.Name, res.StatusCode)
	}

	bytes, err := ioutil.ReadAll(reader)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not read objects %v (%v) API response", request.object.ObjectID, request.object.Name)
	}

	return bytes, nil
}