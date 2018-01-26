package httpx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
)

type HttpRequest struct {
	Method  string
	RawURL  string
	Query   url.Values
	Data    io.Reader
	Headers map[string][]string
}

func (client *HttpClient) sendRequest(ctx context.Context, req *HttpRequest) (*HttpResponse, error) {

	if req == nil {
		return nil, errors.New("client request invalid.")
	}

	ispayload := (req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodPatch)
	if ispayload && req.Data == nil {
		req.Data = bytes.NewReader([]byte{})
	}

	request, err := client.newRequest(req)
	if err != nil {
		return nil, err
	}

	if ispayload && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "text/plain")
	}

	response, err := client.c.Do(request.WithContext(ctx))
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, err
	}

	return &HttpResponse{
		rawurl:     request.URL.String(),
		body:       response.Body,
		header:     response.Header,
		status:     response.Status,
		statuscode: response.StatusCode,
	}, nil
}

func (client *HttpClient) newRequest(req *HttpRequest) (*http.Request, error) {

	rawurl, err := url.Parse(req.RawURL)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	for key := range rawurl.Query() {
		q.Add(key, rawurl.Query().Get(key))
	}

	for key := range req.Query {
		q.Add(key, req.Query.Get(key))
	}

	if len(q) > 0 {
		rawurl.RawQuery = q.Encode()
	}

	request, err := http.NewRequest(req.Method, rawurl.String(), req.Data)
	if err != nil {
		return nil, err
	}

	for key, value := range client.headers {
		request.Header.Set(key, value)
	}

	if req.Headers != nil {
		for key, value := range req.Headers {
			request.Header[key] = value
		}
	}

	if len(client.cookies) != 0 {
		for _, cookie := range client.cookies {
			request.AddCookie(cookie)
		}
	}

	if client.auth.UserName != "" && client.auth.Password != "" {
		request.SetBasicAuth(client.auth.UserName, client.auth.Password)
	}
	return request, nil
}
