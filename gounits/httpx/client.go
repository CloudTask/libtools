package httpx

import "golang.org/x/net/proxy"

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	DefaultClient = NewClient()

	DefaultTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		DisableKeepAlives:     false,
		MaxIdleConns:          http.DefaultTransport.(*http.Transport).MaxIdleConns,
		MaxIdleConnsPerHost:   http.DefaultMaxIdleConnsPerHost,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
		ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
	}

	DefaultPool = &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 200<<10))
		},
	}
)

type basicAuth struct {
	UserName string
	Password string
}

type httpBuffer struct {
	Data    *bytes.Buffer
	Headers map[string][]string
}

type HttpClient struct {
	c       *http.Client
	pool    *sync.Pool
	auth    basicAuth
	cookies []*http.Cookie
	headers map[string]string
}

func NewClient() *HttpClient {

	client := &http.Client{
		Transport: http.DefaultTransport,
	}
	return NewWithClient(client)
}

func NewWithClient(client *http.Client) *HttpClient {

	if client == nil {
		client = http.DefaultClient
	}

	return &HttpClient{
		c:       client,
		pool:    nil,
		auth:    basicAuth{},
		cookies: make([]*http.Cookie, 0),
		headers: make(map[string]string),
	}
}

func (client *HttpClient) RawClient() *http.Client {

	return client.c
}

func (client *HttpClient) Close() {

	if client.c != nil {
		if transport, ok := client.c.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

func (client *HttpClient) UsePool(pool *sync.Pool) *HttpClient {

	if pool == nil {
		pool = DefaultPool
	}
	client.pool = pool
	return client
}

func (client *HttpClient) GetTransport() *http.Transport {

	if client.c != nil {
		if transport, ok := client.c.Transport.(*http.Transport); ok {
			return transport
		}
	}
	return http.DefaultTransport.(*http.Transport)
}

func (client *HttpClient) SetTransport(transport *http.Transport) *HttpClient {

	if client.c != nil {
		if transport == nil {
			transport = DefaultTransport
		}
		client.c.Transport = transport
	}
	return client
}

func (client *HttpClient) SetBasicAuth(username string, password string) *HttpClient {

	client.auth = basicAuth{
		UserName: username,
		Password: password,
	}
	return client
}

func (client *HttpClient) SetHeader(key string, value string) *HttpClient {

	client.headers[key] = value
	return client
}

func (client *HttpClient) SetHeaders(headers map[string]string) *HttpClient {

	for key, value := range headers {
		client.headers[key] = value
	}
	return client
}

func (client *HttpClient) SetCookie(cookie *http.Cookie) *HttpClient {

	client.cookies = append(client.cookies, cookie)
	return client
}

func (client *HttpClient) SetCookies(cookies []*http.Cookie) *HttpClient {

	client.cookies = append(client.cookies, cookies...)
	return client
}

func (client *HttpClient) SetProxy(proxy *url.URL) *HttpClient {

	client.GetTransport().Proxy = http.ProxyURL(proxy)
	return client
}

func (client *HttpClient) SetSocks5(network string, addr string, auth *proxy.Auth, forward proxy.Dialer) *HttpClient {

	dialer, _ := proxy.SOCKS5(network, addr, auth, forward)
	client.GetTransport().Dial = dialer.Dial
	return client
}

func (client *HttpClient) SetTLSClientConfig(tlsConfig *tls.Config) *HttpClient {

	client.GetTransport().TLSClientConfig = tlsConfig
	return client
}

func Head(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Head(ctx, path, query, headers)
}

func Options(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Options(ctx, path, query, headers)
}

func Get(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Get(ctx, path, query, headers)
}

func Put(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Put(ctx, path, query, data, headers)
}

func PutJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PutJSON(ctx, path, query, object, headers)
}

func PutXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PutXML(ctx, path, query, object, headers)
}

func Post(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Post(ctx, path, query, data, headers)
}

func PostJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PostJSON(ctx, path, query, object, headers)
}

func PostXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PostXML(ctx, path, query, object, headers)
}

func Patch(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Patch(ctx, path, query, data, headers)
}

func PatchJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PatchJSON(ctx, path, query, object, headers)
}

func PatchXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.PatchXML(ctx, path, query, object, headers)
}

func Delete(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return DefaultClient.Delete(ctx, path, query, headers)
}

func (client *HttpClient) Head(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodHead,
		RawURL:  path,
		Query:   query,
		Data:    nil,
		Headers: headers,
	})
}

func (client *HttpClient) Options(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodOptions,
		RawURL:  path,
		Query:   query,
		Data:    nil,
		Headers: headers,
	})
}

func (client *HttpClient) Get(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodGet,
		RawURL:  path,
		Query:   query,
		Data:    nil,
		Headers: headers,
	})
}

func (client *HttpClient) Put(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPut,
		RawURL:  path,
		Query:   query,
		Data:    data,
		Headers: headers,
	})
}

func (client *HttpClient) PutJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeJson(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPut,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) PutXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeXml(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPut,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) Post(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPost,
		RawURL:  path,
		Query:   query,
		Data:    data,
		Headers: headers,
	})
}

func (client *HttpClient) PostJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeJson(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPost,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) PostXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeXml(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPost,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) Patch(ctx context.Context, path string, query url.Values, data io.Reader, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPatch,
		RawURL:  path,
		Query:   query,
		Data:    data,
		Headers: headers,
	})
}

func (client *HttpClient) PatchJSON(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeJson(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPatch,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) PatchXML(ctx context.Context, path string, query url.Values, object interface{}, headers map[string][]string) (*HttpResponse, error) {

	httpBuffer, err := client.encodeXml(object, headers)
	defer client.putBuffer(httpBuffer.Data)
	if err != nil {
		return nil, err
	}

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodPatch,
		RawURL:  path,
		Query:   query,
		Data:    httpBuffer.Data,
		Headers: httpBuffer.Headers,
	})
}

func (client *HttpClient) Delete(ctx context.Context, path string, query url.Values, headers map[string][]string) (*HttpResponse, error) {

	return client.sendRequest(ctx, &HttpRequest{
		Method:  http.MethodDelete,
		RawURL:  path,
		Query:   query,
		Data:    nil,
		Headers: headers,
	})
}

func (client *HttpClient) encodeJson(object interface{}, headers map[string][]string) (*httpBuffer, error) {

	data := client.getBuffer()
	if err := json.NewEncoder(data).Encode(object); err != nil {
		return nil, err
	}

	if headers == nil {
		headers = make(map[string][]string)
	}

	headers["Content-Type"] = []string{"application/json;charset=utf-8"}
	return &httpBuffer{
		Data:    data,
		Headers: headers,
	}, nil
}

func (client *HttpClient) encodeXml(object interface{}, headers map[string][]string) (*httpBuffer, error) {

	data := client.getBuffer()
	if err := xml.NewEncoder(data).Encode(object); err != nil {
		return nil, err
	}

	if headers == nil {
		headers = make(map[string][]string)
	}

	headers["Content-Type"] = []string{"application/xml;charset=utf-8"}
	return &httpBuffer{
		Data:    data,
		Headers: headers,
	}, nil
}

func (client *HttpClient) getBuffer() *bytes.Buffer {

	if client.pool == nil {
		return bytes.NewBuffer([]byte{})
	}

	buf := client.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func (client *HttpClient) putBuffer(buf *bytes.Buffer) {

	if client.pool != nil {
		client.pool.Put(buf)
	}
}
