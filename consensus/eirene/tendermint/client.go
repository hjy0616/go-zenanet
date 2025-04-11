package tendermint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"time"

	abcitypes "github.com/tendermint/tendermint/abci/types"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
	"github.com/zenanetwork/go-zenanet/metrics"
)

var (
	// ErrShutdownDetected is returned if a shutdown was detected
	ErrShutdownDetected      = errors.New("shutdown detected")
	ErrNoResponse            = errors.New("got a nil response")
	ErrNotSuccessfulResponse = errors.New("error while fetching data from Tendermint")
	ErrNotInRejectedList     = errors.New("milestoneID doesn't exist in rejected list")
	ErrNotInMilestoneList    = errors.New("milestoneID doesn't exist in Tendermint")
	ErrServiceUnavailable    = errors.New("service unavailable")
)

const (
	tendermintAPIBodyLimit = 128 * 1024 * 1024 // 128 MB
	stateFetchLimit        = 50
	apiTendermintTimeout   = 5 * time.Second
	retryCall              = 5 * time.Second
)

type StateSyncEventsResponse struct {
	Height string                       `json:"height"`
	Result []*clerk.EventRecordWithTime `json:"result"`
}

type SpanResponse struct {
	Height string              `json:"height"`
	Result span.TendermintSpan `json:"result"`
}

type TendermintClient struct {
	urlString string
	client    http.Client
	closeCh   chan struct{}
}

type Request struct {
	client http.Client
	url    *url.URL
	start  time.Time
}

func NewTendermintClient(urlString string) *TendermintClient {
	return &TendermintClient{
		urlString: urlString,
		client: http.Client{
			Timeout: apiTendermintTimeout,
		},
		closeCh: make(chan struct{}),
	}
}

const (
	fetchStateSyncEventsFormat = "from-id=%d&to-time=%d&limit=%d"
	fetchStateSyncEventsPath   = "clerk/event-record/list"

	fetchCheckpoint      = "/checkpoints/%s"
	fetchCheckpointCount = "/checkpoints/count"

	fetchMilestone      = "/milestone/latest"
	fetchMilestoneCount = "/milestone/count"

	fetchLastNoAckMilestone = "/milestone/lastNoAck"
	fetchNoAckMilestone     = "/milestone/noAck/%s"
	fetchMilestoneID        = "/milestone/ID/%s"

	fetchSpanFormat = "eirene/span/%d"
)

func (h *TendermintClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	eventRecords := make([]*clerk.EventRecordWithTime, 0)

	for {
		url, err := stateSyncURL(h.urlString, fromID, to)
		if err != nil {
			return nil, err
		}

		log.Info("Fetching state sync events", "queryParams", url.RawQuery)

		ctx = withRequestType(ctx, stateSyncRequest)

		response, err := FetchWithRetry[StateSyncEventsResponse](ctx, h.client, url, h.closeCh)
		if err != nil {
			return nil, err
		}

		if response == nil || response.Result == nil {
			// status 204
			break
		}

		eventRecords = append(eventRecords, response.Result...)

		if len(response.Result) < stateFetchLimit {
			break
		}

		fromID += uint64(stateFetchLimit)
	}

	sort.SliceStable(eventRecords, func(i, j int) bool {
		return eventRecords[i].ID < eventRecords[j].ID
	})

	return eventRecords, nil
}

func (h *TendermintClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	url, err := spanURL(h.urlString, spanID)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, spanRequest)

	response, err := FetchWithRetry[SpanResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

// FetchCheckpoint fetches the checkpoint from tendermint
func (h *TendermintClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	url, err := checkpointURL(h.urlString, number)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, checkpointRequest)

	response, err := FetchWithRetry[checkpoint.CheckpointResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

// FetchMilestone fetches the checkpoint from tendermint
func (h *TendermintClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	url, err := milestoneURL(h.urlString)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, milestoneRequest)

	response, err := FetchWithRetry[milestone.MilestoneResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

// FetchCheckpointCount fetches the checkpoint count from tendermint
func (h *TendermintClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	url, err := checkpointCountURL(h.urlString)
	if err != nil {
		return 0, err
	}

	ctx = withRequestType(ctx, checkpointCountRequest)

	response, err := FetchWithRetry[checkpoint.CheckpointCountResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return 0, err
	}

	return response.Result.Result, nil
}

// FetchMilestoneCount fetches the milestone count from tendermint
func (h *TendermintClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	url, err := milestoneCountURL(h.urlString)
	if err != nil {
		return 0, err
	}

	ctx = withRequestType(ctx, milestoneCountRequest)

	response, err := FetchWithRetry[milestone.MilestoneCountResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return 0, err
	}

	return response.Result.Count, nil
}

// FetchLastNoAckMilestone fetches the last no-ack-milestone from tendermint
func (h *TendermintClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	url, err := lastNoAckMilestoneURL(h.urlString)
	if err != nil {
		return "", err
	}

	ctx = withRequestType(ctx, milestoneLastNoAckRequest)

	response, err := FetchWithRetry[milestone.MilestoneLastNoAckResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return "", err
	}

	return response.Result.Result, nil
}

// FetchNoAckMilestone fetches the last no-ack-milestone from tendermint
func (h *TendermintClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	url, err := noAckMilestoneURL(h.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneNoAckRequest)

	response, err := FetchWithRetry[milestone.MilestoneNoAckResponse](ctx, h.client, url, h.closeCh)
	if err != nil {
		return err
	}

	if !response.Result.Result {
		return fmt.Errorf("%w: milestoneID %q", ErrNotInRejectedList, milestoneID)
	}

	return nil
}

// FetchMilestoneID fetches the bool result from Heimdal whether the ID corresponding
// to the given milestone is in process in Tendermint
func (h *TendermintClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	url, err := milestoneIDURL(h.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneIDRequest)

	response, err := FetchWithRetry[milestone.MilestoneIDResponse](ctx, h.client, url, h.closeCh)

	if err != nil {
		return err
	}

	if !response.Result.Result {
		return fmt.Errorf("%w: milestoneID %q", ErrNotInMilestoneList, milestoneID)
	}

	return nil
}

// FetchWithRetry returns data from tendermint with retry
func FetchWithRetry[T any](ctx context.Context, client http.Client, url *url.URL, closeCh chan struct{}) (*T, error) {
	// request data once
	request := &Request{client: client, url: url, start: time.Now()}
	result, err := Fetch[T](ctx, request)

	if err == nil {
		return result, nil
	}

	// 503 (Service Unavailable) is thrown when an endpoint isn't activated
	// yet in tendermint. E.g. when the hardfork hasn't hit yet but tendermint
	// is upgraded.
	if errors.Is(err, ErrServiceUnavailable) {
		log.Debug("Tendermint service unavailable at the moment", "path", url.Path, "error", err)
		return nil, err
	}

	// attempt counter
	attempt := 1

	log.Warn("an error while trying fetching from Tendermint", "path", url.Path, "attempt", attempt, "error", err)

	// create a new ticker for retrying the request
	ticker := time.NewTicker(retryCall)
	defer ticker.Stop()

	const logEach = 5

retryLoop:
	for {
		log.Info("Retrying again in 5 seconds to fetch data from Tendermint", "path", url.Path, "attempt", attempt)

		attempt++

		select {
		case <-ctx.Done():
			log.Debug("Shutdown detected, terminating request by context.Done")

			return nil, ctx.Err()
		case <-closeCh:
			log.Debug("Shutdown detected, terminating request by closing")

			return nil, ErrShutdownDetected
		case <-ticker.C:
			request = &Request{client: client, url: url, start: time.Now()}
			result, err = Fetch[T](ctx, request)

			if errors.Is(err, ErrServiceUnavailable) {
				log.Debug("Tendermint service unavailable at the moment", "path", url.Path, "error", err)
				return nil, err
			}

			if err != nil {
				if attempt%logEach == 0 {
					log.Warn("an error while trying fetching from Tendermint", "path", url.Path, "attempt", attempt, "error", err)
				}

				continue retryLoop
			}

			return result, nil
		}
	}
}

// Fetch returns data from tendermint
func Fetch[T any](ctx context.Context, request *Request) (*T, error) {
	var isSuccessful bool

	defer func() {
		if metrics.Enabled {
			sendMetrics(ctx, request.start, isSuccessful)
		}
	}()

	result := new(T)

	body, err := internalFetchWithTimeout(ctx, request.client, request.url)
	if err != nil {
		return nil, err
	}

	if body == nil {
		return nil, ErrNoResponse
	}

	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, err
	}

	isSuccessful = true

	return result, nil
}

func spanURL(urlString string, spanID uint64) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchSpanFormat, spanID), "")
}

func stateSyncURL(urlString string, fromID uint64, to int64) (*url.URL, error) {
	queryParams := fmt.Sprintf(fetchStateSyncEventsFormat, fromID, to, stateFetchLimit)

	return makeURL(urlString, fetchStateSyncEventsPath, queryParams)
}

func checkpointURL(urlString string, number int64) (*url.URL, error) {
	url := ""
	if number == -1 {
		url = fmt.Sprintf(fetchCheckpoint, "latest")
	} else {
		url = fmt.Sprintf(fetchCheckpoint, fmt.Sprint(number))
	}

	return makeURL(urlString, url, "")
}

func milestoneURL(urlString string) (*url.URL, error) {
	url := fetchMilestone

	return makeURL(urlString, url, "")
}

func checkpointCountURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchCheckpointCount, "")
}

func milestoneCountURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchMilestoneCount, "")
}

func lastNoAckMilestoneURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchLastNoAckMilestone, "")
}

func noAckMilestoneURL(urlString string, id string) (*url.URL, error) {
	url := fmt.Sprintf(fetchNoAckMilestone, id)
	return makeURL(urlString, url, "")
}

func milestoneIDURL(urlString string, id string) (*url.URL, error) {
	url := fmt.Sprintf(fetchMilestoneID, id)
	return makeURL(urlString, url, "")
}

func makeURL(urlString, rawPath, rawQuery string) (*url.URL, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, rawPath)
	u.RawQuery = rawQuery

	return u, err
}

// internal fetch method
func internalFetch(ctx context.Context, client http.Client, u *url.URL) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("%w: response code %d", ErrServiceUnavailable, res.StatusCode)
	}

	// check status code
	if res.StatusCode != 200 && res.StatusCode != 204 {
		return nil, fmt.Errorf("%w: response code %d", ErrNotSuccessfulResponse, res.StatusCode)
	}

	// unmarshall data from buffer
	if res.StatusCode == 204 {
		return nil, nil
	}

	// Limit the number of bytes read from the response body
	limitedBody := http.MaxBytesReader(nil, res.Body, tendermintAPIBodyLimit)

	// get response
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func internalFetchWithTimeout(ctx context.Context, client http.Client, url *url.URL) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, apiTendermintTimeout)
	defer cancel()

	// request data once
	return internalFetch(ctx, client, url)
}

// Close sends a signal to stop the running process
func (h *TendermintClient) Close() {
	close(h.closeCh)
	h.client.CloseIdleConnections()
}

// InitChain implements ITendermintClient interface
func (h *TendermintClient) InitChain(ctx context.Context, req abcitypes.RequestInitChain) (*abcitypes.ResponseInitChain, error) {
	log.Warn("InitChain is not supported in HTTP client mode")
	return &abcitypes.ResponseInitChain{}, nil
}

// BeginBlock implements ITendermintClient interface
func (h *TendermintClient) BeginBlock(ctx context.Context, req abcitypes.RequestBeginBlock) (*abcitypes.ResponseBeginBlock, error) {
	log.Warn("BeginBlock is not supported in HTTP client mode")
	return &abcitypes.ResponseBeginBlock{}, nil
}

// CheckTx implements ITendermintClient interface
func (h *TendermintClient) CheckTx(ctx context.Context, req abcitypes.RequestCheckTx) (*abcitypes.ResponseCheckTx, error) {
	log.Warn("CheckTx is not supported in HTTP client mode")
	return &abcitypes.ResponseCheckTx{}, nil
}

// DeliverTx implements ITendermintClient interface
func (h *TendermintClient) DeliverTx(ctx context.Context, req abcitypes.RequestDeliverTx) (*abcitypes.ResponseDeliverTx, error) {
	log.Warn("DeliverTx is not supported in HTTP client mode")
	return &abcitypes.ResponseDeliverTx{}, nil
}

// EndBlock implements ITendermintClient interface
func (h *TendermintClient) EndBlock(ctx context.Context, req abcitypes.RequestEndBlock) (*abcitypes.ResponseEndBlock, error) {
	log.Warn("EndBlock is not supported in HTTP client mode")
	return &abcitypes.ResponseEndBlock{}, nil
}

// Commit implements ITendermintClient interface
func (h *TendermintClient) Commit(ctx context.Context) (*abcitypes.ResponseCommit, error) {
	log.Warn("Commit is not supported in HTTP client mode")
	return &abcitypes.ResponseCommit{}, nil
}

// GetValidators implements ITendermintClient interface
func (h *TendermintClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	log.Warn("GetValidators is not supported in HTTP client mode")
	return []*valset.Validator{}, nil
}

// GetCurrentValidatorSet implements ITendermintClient interface
func (h *TendermintClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	log.Warn("GetCurrentValidatorSet is not supported in HTTP client mode")
	return valset.NewValidatorSet([]*valset.Validator{}), nil
}

// Connect implements ITendermintClient interface
func (h *TendermintClient) Connect() error {
	// HTTP 클라이언트는 즉시 사용 가능하므로 별도의 연결 필요 없음
	return nil
}

// IsConnected implements ITendermintClient interface
func (h *TendermintClient) IsConnected() bool {
	return true
}
