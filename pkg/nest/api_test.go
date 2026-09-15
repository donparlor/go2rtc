package nest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNextExtendDelay(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		remaining time.Duration
		wantDelay time.Duration
		wantErr   bool
	}{
		{
			name:      "expired",
			remaining: 0,
			wantErr:   true,
		},
		{
			name:      "19 seconds",
			remaining: 19 * time.Second,
			wantErr:   true,
		},
		{
			name:      "49 seconds",
			remaining: 49 * time.Second,
			wantErr:   true,
		},
		{
			name:      "50 seconds",
			remaining: 50 * time.Second,
			wantDelay: 30 * time.Second,
		},
		{
			name:      "51 seconds",
			remaining: 51 * time.Second,
			wantDelay: 30 * time.Second,
		},
		{
			name:      "119 seconds",
			remaining: 119 * time.Second,
			wantDelay: 30 * time.Second,
		},
		{
			name:      "120 seconds",
			remaining: 120 * time.Second,
			wantDelay: 30 * time.Second,
		},
		{
			name:      "121 seconds",
			remaining: 121 * time.Second,
			wantDelay: 31 * time.Second,
		},
		{
			name:      "300 seconds",
			remaining: 300 * time.Second,
			wantDelay: 210 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay, err := nextExtendDelay(now, now.Add(tt.remaining))

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got delay %s", delay)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if delay != tt.wantDelay {
				t.Fatalf("delay = %s, want %s", delay, tt.wantDelay)
			}
		})
	}
}

type httpDoerFunc func(*http.Request) (*http.Response, error)

func (f httpDoerFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestForcedOAuthRefreshBypassesValidCache(t *testing.T) {
	key := "test-client:test-secret:test-refresh"

	cacheMu.Lock()
	cache[key] = &API{
		Token:     "cached-token",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	cacheMu.Unlock()

	t.Cleanup(func() {
		cacheMu.Lock()
		delete(cache, key)
		cacheMu.Unlock()
	})

	calls := 0

	client := httpDoerFunc(func(req *http.Request) (*http.Response, error) {
		calls++

		if req.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", req.Method)
		}

		if got := req.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Fatalf("Content-Type = %q", got)
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read OAuth body: %v", err)
		}

		values, err := url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse OAuth body: %v", err)
		}

		if values.Get("refresh_token") != "test-refresh" {
			t.Fatalf("unexpected synthetic refresh token")
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body: io.NopCloser(strings.NewReader(
				`{"access_token":"fresh-token","expires_in":3600,"token_type":"Bearer"}`,
			)),
		}, nil
	})

	api := &API{
		Token:             "cached-token",
		ExpiresAt:         time.Now().Add(time.Hour),
		key:               key,
		clientID:          "test-client",
		clientSecret:      "test-secret",
		oauthRefreshToken: "test-refresh",
		client:            client,
	}

	if err := api.refreshTokenWithContext(context.Background()); err != nil {
		t.Fatalf("refreshTokenWithContext: %v", err)
	}

	if calls != 1 {
		t.Fatalf("OAuth calls = %d, want 1", calls)
	}

	if api.Token != "fresh-token" {
		t.Fatalf("current token = %q, want fresh-token", api.Token)
	}

	cacheMu.Lock()
	cached := cache[key]
	cacheMu.Unlock()

	if cached == nil || cached.Token != "fresh-token" {
		t.Fatalf("cache was not updated with fresh token")
	}
}

func TestFailedForcedOAuthRefreshPreservesExistingState(t *testing.T) {
	key := "fail-client:fail-secret:fail-refresh"
	oldExpiry := time.Now().Add(time.Hour)

	cacheMu.Lock()
	cache[key] = &API{
		Token:     "old-token",
		ExpiresAt: oldExpiry,
	}
	cacheMu.Unlock()

	t.Cleanup(func() {
		cacheMu.Lock()
		delete(cache, key)
		cacheMu.Unlock()
	})

	client := httpDoerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Internal Server Error",
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, nil
	})

	api := &API{
		Token:             "old-token",
		ExpiresAt:         oldExpiry,
		key:               key,
		clientID:          "fail-client",
		clientSecret:      "fail-secret",
		oauthRefreshToken: "fail-refresh",
		client:            client,
	}

	if err := api.refreshTokenWithContext(context.Background()); err == nil {
		t.Fatal("expected refresh error")
	}

	if api.Token != "old-token" {
		t.Fatalf("current token changed after failed refresh: %q", api.Token)
	}

	if !api.ExpiresAt.Equal(oldExpiry) {
		t.Fatalf("current expiry changed after failed refresh")
	}

	cacheMu.Lock()
	cached := cache[key]
	cacheMu.Unlock()

	if cached == nil {
		t.Fatal("cached token disappeared after failed refresh")
	}

	if cached.Token != "old-token" {
		t.Fatalf("cached token changed after failed refresh: %q", cached.Token)
	}

	if !cached.ExpiresAt.Equal(oldExpiry) {
		t.Fatalf("cached expiry changed after failed refresh")
	}
}

func TestExtendStreamWithContextReturnsHTTPStatus(t *testing.T) {
	api := &API{
		Token:           "synthetic-token",
		StreamProjectID: "project",
		StreamDeviceID:  "device",
		StreamSessionID: "session",
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
		client: httpDoerFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Status:     "401 Unauthorized",
				Body:       io.NopCloser(strings.NewReader(`{}`)),
			}, nil
		}),
	}

	err := api.extendStreamWithContext(context.Background())

	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v, want *httpStatusError", err)
	}

	if statusErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf(
			"status code = %d, want %d",
			statusErr.StatusCode,
			http.StatusUnauthorized,
		)
	}
}

func TestExtendStreamWithContextCancellation(t *testing.T) {
	started := make(chan struct{})

	api := &API{
		Token:           "synthetic-token",
		StreamProjectID: "project",
		StreamDeviceID:  "device",
		StreamSessionID: "session",
		client: httpDoerFunc(func(req *http.Request) (*http.Response, error) {
			close(started)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- api.extendStreamWithContext(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not start")
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("extension request was not canceled promptly")
	}
}

func TestRetryNestOperationTransientThenSuccess(t *testing.T) {
	calls := 0
	var delays []time.Duration

	err := retryNestOperation(
		context.Background(),
		func(context.Context) error {
			calls++
			if calls == 1 {
				return &httpStatusError{
					StatusCode: http.StatusInternalServerError,
					Status:     "500 Internal Server Error",
				}
			}
			return nil
		},
		func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}

	if len(delays) != 1 || delays[0] != 2*time.Second {
		t.Fatalf("delays = %v, want [2s]", delays)
	}
}

func TestRetryNestOperationTwoFailuresThenSuccess(t *testing.T) {
	calls := 0
	var delays []time.Duration

	err := retryNestOperation(
		context.Background(),
		func(context.Context) error {
			calls++
			if calls < 3 {
				return &httpStatusError{
					StatusCode: http.StatusServiceUnavailable,
					Status:     "503 Service Unavailable",
				}
			}
			return nil
		},
		func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}

	want := []time.Duration{
		2 * time.Second,
		5 * time.Second,
	}

	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}

	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

func TestRetryNestOperationExhaustion(t *testing.T) {
	calls := 0
	var delays []time.Duration

	err := retryNestOperation(
		context.Background(),
		func(context.Context) error {
			calls++
			return &httpStatusError{
				StatusCode: http.StatusTooManyRequests,
				Status:     "429 Too Many Requests",
			}
		},
		func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	)

	if err == nil {
		t.Fatal("expected final error")
	}

	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}

	if len(delays) != 2 ||
		delays[0] != 2*time.Second ||
		delays[1] != 5*time.Second {
		t.Fatalf("delays = %v, want [2s 5s]", delays)
	}
}

func TestRetryNestOperationStopDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitStarted := make(chan struct{})
	done := make(chan error, 1)

	calls := 0

	go func() {
		done <- retryNestOperation(
			ctx,
			func(context.Context) error {
				calls++
				return &httpStatusError{
					StatusCode: http.StatusServiceUnavailable,
					Status:     "503 Service Unavailable",
				}
			},
			func(ctx context.Context, _ time.Duration) error {
				close(waitStarted)
				<-ctx.Done()
				return ctx.Err()
			},
		)
	}()

	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("retry backoff did not start")
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not stop after cancellation")
	}

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestExtendStreamWithAuth401ForcesOAuthRefresh(t *testing.T) {
	const key = "test-client:test-secret:test-refresh"

	cacheMu.Lock()
	oldCache := cache[key]
	cache[key] = &API{
		Token:     "cached-old-token",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	cacheMu.Unlock()

	defer func() {
		cacheMu.Lock()
		if oldCache == nil {
			delete(cache, key)
		} else {
			cache[key] = oldCache
		}
		cacheMu.Unlock()
	}()

	extensionCalls := 0
	oauthCalls := 0

	api := &API{
		Token:             "old-token",
		ExpiresAt:         time.Now().Add(time.Hour),
		StreamProjectID:   "project",
		StreamDeviceID:    "device",
		StreamSessionID:   "session-1",
		StreamExpiresAt:   time.Now().Add(5 * time.Minute),
		key:               key,
		clientID:          "test-client",
		clientSecret:      "test-secret",
		oauthRefreshToken: "test-refresh",
	}

	api.client = httpDoerFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Host {
		case "smartdevicemanagement.googleapis.com":
			extensionCalls++

			if extensionCalls == 1 {
				if got := req.Header.Get("Authorization"); got != "Bearer old-token" {
					t.Fatalf(
						"first Authorization = %q, want %q",
						got,
						"Bearer old-token",
					)
				}

				return &http.Response{
					StatusCode: http.StatusUnauthorized,
					Status:     "401 Unauthorized",
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			}

			if got := req.Header.Get("Authorization"); got != "Bearer fresh-token" {
				t.Fatalf(
					"second Authorization = %q, want %q",
					got,
					"Bearer fresh-token",
				)
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body: io.NopCloser(strings.NewReader(
					`{"results":{"expiresAt":"2099-01-01T00:00:00Z","mediaSessionId":"session-2"}}`,
				)),
			}, nil

		case "www.googleapis.com":
			oauthCalls++

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body: io.NopCloser(strings.NewReader(
					`{"access_token":"fresh-token","expires_in":3600,"token_type":"Bearer"}`,
				)),
			}, nil

		default:
			t.Fatalf("unexpected host: %s", req.URL.Host)
			return nil, nil
		}
	})

	err := api.extendStreamWithAuth(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if extensionCalls != 2 {
		t.Fatalf("extension calls = %d, want 2", extensionCalls)
	}

	if oauthCalls != 1 {
		t.Fatalf("OAuth calls = %d, want 1", oauthCalls)
	}

	if api.Token != "fresh-token" {
		t.Fatalf("API token = %q, want fresh-token", api.Token)
	}

	if api.StreamSessionID != "session-2" {
		t.Fatalf(
			"session ID = %q, want session-2",
			api.StreamSessionID,
		)
	}

	cacheMu.Lock()
	cached := cache[key]
	cacheMu.Unlock()

	if cached == nil || cached.Token != "fresh-token" {
		t.Fatalf("cached OAuth token was not refreshed")
	}
}

func TestExtendStreamWithAuth403DoesNotRefreshOAuth(t *testing.T) {
	extensionCalls := 0
	oauthCalls := 0

	api := &API{
		Token:             "old-token",
		ExpiresAt:         time.Now().Add(time.Hour),
		StreamProjectID:   "project",
		StreamDeviceID:    "device",
		StreamSessionID:   "session",
		key:               "client:secret:refresh",
		clientID:          "client",
		clientSecret:      "secret",
		oauthRefreshToken: "refresh",
	}

	api.client = httpDoerFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "www.googleapis.com" {
			oauthCalls++
		}

		extensionCalls++

		return &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, nil
	})

	err := api.extendStreamWithAuth(context.Background())

	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v, want *httpStatusError", err)
	}

	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf(
			"status code = %d, want %d",
			statusErr.StatusCode,
			http.StatusForbidden,
		)
	}

	if extensionCalls != 1 {
		t.Fatalf("extension calls = %d, want 1", extensionCalls)
	}

	if oauthCalls != 0 {
		t.Fatalf("OAuth calls = %d, want 0", oauthCalls)
	}
}

func TestExtendRunnerImmediateStop(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	api.StartExtendStreamTimer(nil)

	done := make(chan struct{})
	go func() {
		api.StopExtendStreamTimer()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StopExtendStreamTimer did not return promptly")
	}
}

func TestExtendRunnerDoubleStop(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	api.StartExtendStreamTimer(nil)

	api.StopExtendStreamTimer()

	done := make(chan struct{})
	go func() {
		api.StopExtendStreamTimer()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second StopExtendStreamTimer blocked")
	}
}

func TestExtendRunnerConcurrentStops(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	api.StartExtendStreamTimer(nil)

	const goroutines = 16

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			api.StopExtendStreamTimer()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent StopExtendStreamTimer calls blocked")
	}
}

func TestExtendRunnerRestartAfterStop(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	api.StartExtendStreamTimer(nil)

	api.extendMu.Lock()
	firstDone := api.extendDone
	api.extendMu.Unlock()

	if firstDone == nil {
		t.Fatal("first runner did not start")
	}

	api.StopExtendStreamTimer()

	api.extendMu.Lock()
	cancelAfterStop := api.extendCancel
	doneAfterStop := api.extendDone
	api.extendMu.Unlock()

	if cancelAfterStop != nil || doneAfterStop != nil {
		t.Fatal("runner state was not cleared after Stop")
	}

	api.StreamExpiresAt = time.Now().Add(5 * time.Minute)
	api.StartExtendStreamTimer(nil)

	api.extendMu.Lock()
	secondDone := api.extendDone
	api.extendMu.Unlock()

	if secondDone == nil {
		t.Fatal("second runner did not start")
	}

	if secondDone == firstDone {
		t.Fatal("restart reused the previous runner done channel")
	}

	api.StopExtendStreamTimer()
}

func TestExtendRunnerDoubleStartIsIdempotent(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	api.StartExtendStreamTimer(nil)

	api.extendMu.Lock()
	firstDone := api.extendDone
	firstCancelPresent := api.extendCancel != nil
	api.extendMu.Unlock()

	if firstDone == nil || !firstCancelPresent {
		t.Fatal("first runner did not start")
	}

	api.StartExtendStreamTimer(nil)

	api.extendMu.Lock()
	secondDone := api.extendDone
	secondCancelPresent := api.extendCancel != nil
	api.extendMu.Unlock()

	if secondDone != firstDone {
		t.Fatal("second StartExtendStreamTimer started another runner")
	}

	if !secondCancelPresent {
		t.Fatal("runner cancel function unexpectedly disappeared")
	}

	api.StopExtendStreamTimer()
}

func TestExtendRunnerExpiredSessionCallsFatal(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(-time.Second),
	}

	fatal := make(chan error, 1)

	api.StartExtendStreamTimer(func(err error) {
		fatal <- err
	})

	select {
	case err := <-fatal:
		if err == nil {
			t.Fatal("fatal callback received nil error")
		}

		if !strings.Contains(err.Error(), "stream session expired") {
			t.Fatalf(
				"fatal error = %q, want stream session expired",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("expired session did not trigger fatal callback")
	}

	// Ensure the runner has completely exited and its state is cleared.
	api.StopExtendStreamTimer()

	api.extendMu.Lock()
	cancel := api.extendCancel
	done := api.extendDone
	api.extendMu.Unlock()

	if cancel != nil || done != nil {
		t.Fatal("fatal runner did not clear its state")
	}
}

func TestExtendRunnerNormalStopDoesNotCallFatal(t *testing.T) {
	api := &API{
		StreamExpiresAt: time.Now().Add(5 * time.Minute),
	}

	fatal := make(chan error, 1)

	api.StartExtendStreamTimer(func(err error) {
		fatal <- err
	})

	api.StopExtendStreamTimer()

	select {
	case err := <-fatal:
		t.Fatalf("normal Stop triggered fatal callback: %v", err)
	default:
	}
}

func TestExtendFatalSuppressedWhenNormalStopWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	api := &API{
		extendCancel: cancel,
		extendDone:   done,
	}

	fatal := make(chan error, 1)
	stopDone := make(chan struct{})

	go func() {
		api.StopExtendStreamTimer()
		close(stopDone)
	}()

	// StopExtendStreamTimer calls cancel() while holding extendMu.
	// Once ctx.Done() is closed, the normal-stop decision has won.
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("normal Stop did not cancel the runner context")
	}

	api.callExtendFatal(
		ctx,
		done,
		func(err error) {
			fatal <- err
		},
		errors.New("synthetic fatal"),
	)

	select {
	case err := <-fatal:
		t.Fatalf(
			"fatal callback ran after normal Stop won: %v",
			err,
		)
	default:
	}

	// Simulate the runner defer closing done so Stop can return.
	close(done)

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after runner completion")
	}
}

func TestExtendFatalWinsBeforeNormalStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	api := &API{
		extendCancel: cancel,
		extendDone:   done,
	}

	fatal := make(chan error, 1)
	wantErr := errors.New("synthetic fatal")

	api.callExtendFatal(
		ctx,
		done,
		func(err error) {
			fatal <- err
		},
		wantErr,
	)

	select {
	case err := <-fatal:
		if !errors.Is(err, wantErr) {
			t.Fatalf(
				"fatal callback error = %v, want %v",
				err,
				wantErr,
			)
		}
	default:
		t.Fatal("fatal callback did not run")
	}

	api.extendMu.Lock()
	cancelAfterFatal := api.extendCancel
	doneAfterFatal := api.extendDone
	api.extendMu.Unlock()

	if cancelAfterFatal != nil {
		t.Fatal("fatal commit did not clear extendCancel")
	}

	if doneAfterFatal != done {
		t.Fatal("fatal commit cleared extendDone before runner exit")
	}

	stopDone := make(chan struct{})

	go func() {
		api.StopExtendStreamTimer()
		close(stopDone)
	}()

	// Fatal already won, so Stop must wait for runner completion.
	select {
	case <-stopDone:
		t.Fatal("Stop returned before fatal runner completion")
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the runner defer closing done.
	close(done)

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after fatal runner completion")
	}
}
