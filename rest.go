// Copyright 2025 Vasiliy Vdovin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rest

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

const dead = time.Hour

type API interface {

	// Adds a request limit.
	Limit(per time.Duration, limit int)

	// Creates a new get request.
	Get(path string, weight int) Request

	// Set interval between requests
	Interval(time.Duration)

	// Sets the base path for all requests.
	// Base(string)
}

type response struct {
	data []byte
	error
}

func (r *response) read() ([]byte, error) {
	return r.data, r.error
}
func (r *response) writte(data []byte, err error) {
	r.data = data
	r.error = err
}

type api struct {
	con *tls.Conn      // Connection to endpoint.
	csr chan *request  // Channel for sending requests.
	cgr chan *response // Channel for getting response
	hst string         // Host.
	prt string         // Port number.
	hdr http.Header    // Default headers applied to all requests.
	ibr time.Duration  // Interval between requests.
	lms limiters       // Limits for endpoint requests.
	crt int            // Count of  request.
	tlc *tls.Config    // TLS config for dial; nil means the default. Tests set it.

	tmp struct {
		rsp *http.Response // Server response before parsing.
		brd *bufio.Reader  // Used when parsing a response.
	}
}

// Creates a thread-safe REST client.
func NewAPI(endpoint string) (API, error) {

	a := &api{}
	a.hdr = make(http.Header)
	a.csr = make(chan *request)
	a.cgr = make(chan *response)
	a.ibr = 0
	a.lms.init()

	a.tmp.brd = bufio.NewReader(nil)

	// Parsing the URL.
	u, e := url.Parse(endpoint)

	if e != nil {
		return nil, e
	}

	a.hst = u.Hostname()
	a.prt = u.Port()

	if a.prt == "" {
		a.prt = "443"
	}

	if u.Scheme != "https" {
		return nil, fmt.Errorf("invalid scheme: %s", u.Scheme)
	}

	if len(a.hst) < 1 || len(a.prt) < 1 {
		return nil, errors.New("this connection is not binded to a host")
	}

	go func() {

		for csr := range a.csr {

			rsp := &response{}
			rsp.writte(a.send(csr))

			a.cgr <- rsp
		}
	}()

	// Setting the default headers.
	a.hdr.Set("Host", u.Host)
	a.hdr.Set("User-Agent", "github.com/pequin/rest")
	a.hdr.Set("Content-Type", "application/json")
	a.hdr.Set("Accept", "*/*")
	a.hdr.Set("Connection", "keep-alive")

	return a, nil
}

func (a *api) dial() error {

	var err error

	cfg := a.tlc
	if cfg == nil {
		cfg = &tls.Config{}
	}

	a.con, err = tls.Dial("tcp", a.host(), cfg)
	if err != nil {
		a.con = nil
		return err
	}

	a.tmp.brd.Reset(a.con)

	err = a.con.SetDeadline(time.Now().Add(dead))

	if err != nil {
		e := a.con.Close()
		if e != nil {
			return e
		}
	}

	return err
}

func (a *api) host() string {
	return net.JoinHostPort(a.hst, a.prt)
}

func (a *api) send(r *request) ([]byte, error) {

	var err error

	// waiting for interval between requests.
	if a.crt > 0 && a.ibr > 0 {
		time.Sleep(a.ibr)
	}

	// Waiting for the limit to expire.
	if err = a.lms.wait(r.wgt); err != nil {
		return nil, fmt.Errorf("path: %s, %w", r.url(), err)
	}

	// If the dial() was not called.
	if a.con == nil {
		err = a.dial()
		if err != nil {
			return nil, err
		}
	}

	raw := r.encode()

	if _, err = a.con.Write(raw); err != nil {

		var ne net.Error
		if !(errors.As(err, &ne) && ne.Timeout()) && !errors.Is(err, net.ErrClosed) {
			return nil, err
		}

		// The connection timed out or was closed: dial again and resend once.
		if err = a.dial(); err != nil {
			return nil, err
		}
		if _, err = a.con.Write(raw); err != nil {
			return nil, err
		}
	}

	err = a.con.SetDeadline(time.Now().Add(dead))
	if err != nil {
		e := a.con.Close()
		if e != nil {
			return nil, e
		}
	}

	a.tmp.rsp, err = http.ReadResponse(a.tmp.brd, nil)
	if err != nil {
		return nil, err
	}
	defer a.tmp.rsp.Body.Close()

	if a.tmp.rsp.StatusCode != 200 {
		// Drain the body so the next response starts at the right offset.
		if _, err = io.Copy(io.Discard, a.tmp.rsp.Body); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("status:%d of the response to the request:%s", a.tmp.rsp.StatusCode, r.url())
	}

	a.crt++

	return io.ReadAll(a.tmp.rsp.Body)

}

// Adds a request limit
func (a *api) Limit(per time.Duration, limit int) {

	a.lms.add(per, limit)

}

// Set interval between requests
func (a *api) Interval(interval time.Duration) {

	a.ibr = interval
}

// Creates a new get request.
func (a *api) Get(path string, weight int) Request {
	r := &request{}
	r.api = a

	r.init(http.MethodGet, path, weight)

	return r
}
