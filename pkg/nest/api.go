package nest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	nestHTTPTimeout     = 15 * time.Second
	nestExtendLead      = 90 * time.Second
	nestExtendMinDelay  = 30 * time.Second
	nestExtendMinMargin = 20 * time.Second
)

func nextExtendDelay(now, expiresAt time.Time) (time.Duration, error) {
	remaining := expiresAt.Sub(now)
	if remaining <= 0 {
		return 0, errors.New("nest: stream session expired")
	}

	delay := remaining - nestExtendLead
	if delay < nestExtendMinDelay {
		delay = nestExtendMinDelay
	}

	if remaining-delay < nestExtendMinMargin {
		return 0, errors.New("nest: stream session too close to expiry")
	}

	return delay, nil
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type oauthToken struct {
	Token     string
	ExpiresAt time.Time
}

type API struct {
	Token     string
	ExpiresAt time.Time

	StreamProjectID string
	StreamDeviceID  string
	StreamExpiresAt time.Time

	// WebRTC
	StreamSessionID string

	// RTSP
	StreamToken          string
	StreamExtensionToken string

	key               string // credentials cache key, used to refresh the OAuth token
	clientID          string
	clientSecret      string
	oauthRefreshToken string
	client            httpDoer

	extendMu     sync.Mutex
	extendCancel context.CancelFunc
	extendDone   chan struct{}
}

type Auth struct {
	AccessToken string
}

type DeviceInfo struct {
	Name      string
	DeviceID  string
	Protocols []string
}

var cache = map[string]*API{}
var cacheMu sync.Mutex

func requestOAuthToken(
	ctx context.Context,
	client httpDoer,
	clientID, clientSecret, refreshToken string,
	now time.Time,
) (oauthToken, error) {
	data := url.Values{
		"grant_type":    []string{"refresh_token"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
		"refresh_token": []string{refreshToken},
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://www.googleapis.com/oauth2/v4/token",
		strings.NewReader(data.Encode()),
	)
	if err != nil {
		return oauthToken{}, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	res, err := client.Do(req)
	if err != nil {
		return oauthToken{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return oauthToken{}, &httpStatusError{
			StatusCode: res.StatusCode,
			Status:     res.Status,
		}
	}

	var resv struct {
		AccessToken string        `json:"access_token"`
		ExpiresIn   time.Duration `json:"expires_in"`
		Scope       string        `json:"scope"`
		TokenType   string        `json:"token_type"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return oauthToken{}, err
	}

	return oauthToken{
		Token:     resv.AccessToken,
		ExpiresAt: now.Add(resv.ExpiresIn * time.Second),
	}, nil
}

func NewAPI(clientID, clientSecret, refreshToken string) (*API, error) {
	key := clientID + ":" + clientSecret + ":" + refreshToken
	now := time.Now()
	client := &http.Client{Timeout: nestHTTPTimeout}

	cacheMu.Lock()
	if cached := cache[key]; cached != nil && now.Before(cached.ExpiresAt) {
		token := cached.Token
		expiresAt := cached.ExpiresAt
		cacheMu.Unlock()

		return &API{
			Token:             token,
			ExpiresAt:         expiresAt,
			key:               key,
			clientID:          clientID,
			clientSecret:      clientSecret,
			oauthRefreshToken: refreshToken,
			client:            client,
		}, nil
	}
	cacheMu.Unlock()

	token, err := requestOAuthToken(
		context.Background(),
		client,
		clientID,
		clientSecret,
		refreshToken,
		now,
	)
	if err != nil {
		return nil, err
	}

	cacheMu.Lock()
	cache[key] = &API{
		Token:     token.Token,
		ExpiresAt: token.ExpiresAt,
	}
	cacheMu.Unlock()

	return &API{
		Token:             token.Token,
		ExpiresAt:         token.ExpiresAt,
		key:               key,
		clientID:          clientID,
		clientSecret:      clientSecret,
		oauthRefreshToken: refreshToken,
		client:            client,
	}, nil
}

func (a *API) GetDevices(projectID string) ([]DeviceInfo, error) {
	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" + projectID + "/devices"
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: nestHTTPTimeout}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Devices []Device
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return nil, err
	}

	devices := make([]DeviceInfo, 0, len(resv.Devices))

	for _, device := range resv.Devices {
		// only RTSP and WEB_RTC available (both supported)
		if len(device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols) == 0 {
			continue
		}

		i := strings.LastIndexByte(device.Name, '/')
		if i <= 0 {
			continue
		}

		name := device.Traits.SdmDevicesTraitsInfo.CustomName
		// Devices configured through the Nest app use the container/room name as opposed to the customName trait
		if name == "" && len(device.ParentRelations) > 0 {
			name = device.ParentRelations[0].DisplayName
		}

		devices = append(devices, DeviceInfo{
			Name:      name,
			DeviceID:  device.Name[i+1:],
			Protocols: device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols,
		})
	}

	return devices, nil
}

func (a *API) ExchangeSDP(projectID, deviceID, offer string) (string, error) {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			Offer string `json:"offerSdp"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream"
	reqv.Params.Offer = offer

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"

	maxRetries := 3
	retryDelay := time.Second * 30

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return "", err
		}

		req.Header.Set("Authorization", "Bearer "+a.Token)

		client := &http.Client{Timeout: nestHTTPTimeout}
		res, err := client.Do(req)
		if err != nil {
			return "", err
		}

		// Handle 409 (Conflict), 429 (Too Many Requests), and 401 (Unauthorized)
		if res.StatusCode == 409 || res.StatusCode == 429 || res.StatusCode == 401 {
			res.Body.Close()
			if attempt < maxRetries-1 {
				// Get new token from Google
				if err := a.refreshToken(); err != nil {
					return "", err
				}
				time.Sleep(retryDelay)
				retryDelay *= 2 // exponential backoff
				continue
			}
		}

		defer res.Body.Close()

		if res.StatusCode != 200 {
			return "", errors.New("nest: wrong status: " + res.Status)
		}

		var resv struct {
			Results struct {
				Answer         string    `json:"answerSdp"`
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}

		if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
			return "", err
		}

		a.StreamProjectID = projectID
		a.StreamDeviceID = deviceID
		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt

		return resv.Results.Answer, nil
	}

	return "", errors.New("nest: max retries exceeded")
}

func (a *API) refreshToken() error {
	return a.refreshTokenWithContext(context.Background())
}

func (a *API) refreshTokenWithContext(ctx context.Context) error {
	if a.key == "" ||
		a.clientID == "" ||
		a.clientSecret == "" ||
		a.oauthRefreshToken == "" {
		return errors.New("nest: missing OAuth credentials")
	}

	client := a.client
	if client == nil {
		client = &http.Client{Timeout: nestHTTPTimeout}
	}

	token, err := requestOAuthToken(
		ctx,
		client,
		a.clientID,
		a.clientSecret,
		a.oauthRefreshToken,
		time.Now(),
	)
	if err != nil {
		return err
	}

	// Publish only after the OAuth request has fully succeeded.
	a.Token = token.Token
	a.ExpiresAt = token.ExpiresAt

	cacheMu.Lock()
	cache[a.key] = &API{
		Token:     token.Token,
		ExpiresAt: token.ExpiresAt,
	}
	cacheMu.Unlock()

	return nil
}

type httpStatusError struct {
	StatusCode int
	Status     string
}

func (e *httpStatusError) Error() string {
	return "nest: wrong status: " + e.Status
}

func isRetryableNestError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		switch statusErr.StatusCode {
		case http.StatusRequestTimeout,
			http.StatusConflict,
			http.StatusTooManyRequests:
			return true
		}

		return statusErr.StatusCode >= 500
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nestRetryDelay(attempt int) time.Duration {
	if attempt == 0 {
		return 2 * time.Second
	}

	return 5 * time.Second
}

func retryNestOperation(
	ctx context.Context,
	operation func(context.Context) error,
	wait func(context.Context, time.Duration) error,
) error {
	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := operation(ctx)
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryableNestError(err) || attempt == 2 {
			return err
		}

		if err = wait(ctx, nestRetryDelay(attempt)); err != nil {
			return err
		}
	}

	return lastErr
}

func (a *API) extendStreamWithAuth(ctx context.Context) error {
	// Long-lived sessions may outlive the OAuth token.
	if time.Now().After(a.ExpiresAt.Add(-30 * time.Second)) {
		if err := a.refreshTokenWithContext(ctx); err != nil {
			return err
		}
	}

	err := a.extendStreamWithContext(ctx)
	if err == nil {
		return nil
	}

	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) ||
		statusErr.StatusCode != http.StatusUnauthorized {
		return err
	}

	// A 401 must force a real OAuth refresh outside the cache.
	if err = a.refreshTokenWithContext(ctx); err != nil {
		return err
	}

	return a.extendStreamWithContext(ctx)
}

func (a *API) extendStreamWithRetry(ctx context.Context) error {
	return retryNestOperation(
		ctx,
		a.extendStreamWithAuth,
		waitContext,
	)
}

func (a *API) ExtendStream() error {
	return a.extendStreamWithContext(context.Background())
}

func (a *API) extendStreamWithContext(ctx context.Context) error {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			MediaSessionID       string `json:"mediaSessionId,omitempty"`
			StreamExtensionToken string `json:"streamExtensionToken,omitempty"`
		} `json:"params"`
	}

	if a.StreamToken != "" {
		// RTSP
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendRtspStream"
		reqv.Params.StreamExtensionToken = a.StreamExtensionToken
	} else {
		// WebRTC
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendWebRtcStream"
		reqv.Params.MediaSessionID = a.StreamSessionID
	}

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := a.client
	if client == nil {
		client = &http.Client{Timeout: nestHTTPTimeout}
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return &httpStatusError{
			StatusCode: res.StatusCode,
			Status:     res.Status,
		}
	}

	var resv struct {
		Results struct {
			ExpiresAt            time.Time `json:"expiresAt"`
			MediaSessionID       string    `json:"mediaSessionId"`
			StreamExtensionToken string    `json:"streamExtensionToken"`
			StreamToken          string    `json:"streamToken"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return err
	}

	a.StreamSessionID = resv.Results.MediaSessionID
	a.StreamExpiresAt = resv.Results.ExpiresAt
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamToken = resv.Results.StreamToken

	return nil
}

func (a *API) GenerateRtspStream(projectID, deviceID string) (string, error) {
	var reqv struct {
		Command string   `json:"command"`
		Params  struct{} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateRtspStream"

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: nestHTTPTimeout}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}

	if res.StatusCode != 200 {
		return "", errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Results struct {
			StreamURLs           map[string]string `json:"streamUrls"`
			StreamExtensionToken string            `json:"streamExtensionToken"`
			StreamToken          string            `json:"streamToken"`
			ExpiresAt            time.Time         `json:"expiresAt"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return "", err
	}

	if _, ok := resv.Results.StreamURLs["rtspUrl"]; !ok {
		return "", errors.New("nest: failed to generate rtsp url")
	}

	a.StreamProjectID = projectID
	a.StreamDeviceID = deviceID
	a.StreamToken = resv.Results.StreamToken
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamExpiresAt = resv.Results.ExpiresAt

	return resv.Results.StreamURLs["rtspUrl"], nil
}

func (a *API) StopRTSPStream() error {
	if a.StreamProjectID == "" || a.StreamDeviceID == "" {
		return errors.New("nest: tried to stop rtsp stream without a project or device ID")
	}

	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			StreamExtensionToken string `json:"streamExtensionToken"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.StopRtspStream"
	reqv.Params.StreamExtensionToken = a.StreamExtensionToken

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: nestHTTPTimeout}
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New("nest: wrong status: " + res.Status)
	}

	a.StreamProjectID = ""
	a.StreamDeviceID = ""
	a.StreamExtensionToken = ""
	a.StreamToken = ""

	return nil
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
	//Assignee string `json:"assignee"`
	Traits struct {
		SdmDevicesTraitsInfo struct {
			CustomName string `json:"customName"`
		} `json:"sdm.devices.traits.Info"`
		SdmDevicesTraitsCameraLiveStream struct {
			VideoCodecs        []string `json:"videoCodecs"`
			AudioCodecs        []string `json:"audioCodecs"`
			SupportedProtocols []string `json:"supportedProtocols"`
		} `json:"sdm.devices.traits.CameraLiveStream"`
		//SdmDevicesTraitsCameraImage struct {
		//	MaxImageResolution struct {
		//		Width  int `json:"width"`
		//		Height int `json:"height"`
		//	} `json:"maxImageResolution"`
		//} `json:"sdm.devices.traits.CameraImage"`
		//SdmDevicesTraitsCameraPerson struct {
		//} `json:"sdm.devices.traits.CameraPerson"`
		//SdmDevicesTraitsCameraMotion struct {
		//} `json:"sdm.devices.traits.CameraMotion"`
		//SdmDevicesTraitsDoorbellChime struct {
		//} `json:"sdm.devices.traits.DoorbellChime"`
		//SdmDevicesTraitsCameraClipPreview struct {
		//} `json:"sdm.devices.traits.CameraClipPreview"`
	} `json:"traits"`
	ParentRelations []struct {
		Parent      string `json:"parent"`
		DisplayName string `json:"displayName"`
	} `json:"parentRelations"`
}

func (a *API) StartExtendStreamTimer(onFatal func(error)) {
	a.extendMu.Lock()

	// extendDone stays non-nil until the current runner has completely
	// exited, including any fatal callback.
	if a.extendDone != nil {
		a.extendMu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	a.extendCancel = cancel
	a.extendDone = done

	a.extendMu.Unlock()

	go a.runExtendStreamTimer(ctx, done, onFatal)
}

func (a *API) callExtendFatal(
	ctx context.Context,
	done chan struct{},
	onFatal func(error),
	err error,
) {
	if onFatal == nil {
		return
	}

	a.extendMu.Lock()

	// StopExtendStreamTimer cancels the context while holding this same
	// mutex. Whichever path acquires the mutex first determines whether
	// this is a normal stop or a fatal runner termination.
	if a.extendDone != done ||
		a.extendCancel == nil ||
		ctx.Err() != nil {
		a.extendMu.Unlock()
		return
	}

	// Fatal termination is now committed. Keep extendDone non-nil so a
	// second runner cannot start before this one has completely exited.
	a.extendCancel = nil

	a.extendMu.Unlock()

	onFatal(err)
}

func (a *API) runExtendStreamTimer(
	ctx context.Context,
	done chan struct{},
	onFatal func(error),
) {
	defer func() {
		a.extendMu.Lock()

		if a.extendDone == done {
			a.extendCancel = nil
			a.extendDone = nil
		}

		a.extendMu.Unlock()
		close(done)
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		delay, err := nextExtendDelay(
			time.Now(),
			a.StreamExpiresAt,
		)
		if err != nil {
			a.callExtendFatal(ctx, done, onFatal, err)
			return
		}

		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}

		err = a.extendStreamWithRetry(ctx)
		if err == nil {
			continue
		}

		if errors.Is(err, context.Canceled) {
			return
		}

		a.callExtendFatal(ctx, done, onFatal, err)
		return
	}
}

func (a *API) StopExtendStreamTimer() {
	a.extendMu.Lock()

	cancel := a.extendCancel
	done := a.extendDone

	// Publish normal cancellation while holding the same mutex used by
	// callExtendFatal. This closes the Stop-vs-fatal decision window.
	if cancel != nil {
		cancel()
	}

	a.extendMu.Unlock()

	// Never wait for the runner while holding extendMu.
	if done != nil {
		<-done
	}
}
