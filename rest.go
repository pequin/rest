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
	"strings"
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

	tmp struct {
		rsp *http.Response // Server response before parsing.
		brd *bufio.Reader  // Used when parsing a response.
		str string         // Used to temporarily store a string before parsing it into the desired type.
		lrt time.Time      // The response time of the last request.
		rap *response      // Response after parsing
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
	a.tmp.rap = &response{}

	// Parsing the URL.
	u, e := url.Parse(endpoint)

	if e != nil {
		return nil, e
	}

	a.hst, _ = strings.CutPrefix(u.Host, "www.")
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

			a.tmp.rap.writte(a.send(csr))

			a.cgr <- a.tmp.rap
		}
	}()

	// Setting the default headers.
	a.hdr.Set("Host", a.hst)
	a.hdr.Set("User-Agent", "github.com/pequin/rest")
	a.hdr.Set("Content-Type", "application/json")
	a.hdr.Set("Accept", "*/*")
	a.hdr.Set("Connection", "keep-alive")
	a.hdr.Add("Accept-Encoding", "deflate")
	a.hdr.Add("Accept-Encoding", "br")

	return a, nil
}

func (a *api) dial() error {

	var err error

	a.con, err = tls.Dial("tcp", fmt.Sprintf("%s:%s", a.hst, a.prt), &tls.Config{
		InsecureSkipVerify: false,
	})

	if err != nil && a.con != nil {
		e := a.con.Close()
		if e != nil {
			return e
		}
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
	return fmt.Sprintf("%s:%s", a.hst, a.prt)
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

	if _, err = a.con.Write(r.encode()); err != nil {

		if e, f := err.(net.Error); (f && e.Timeout()) || errors.Is(err, net.ErrClosed) {
			err = a.dial()
			if err != nil {
				return nil, err
			}
			a.send(r)
		}

		return nil, err
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
		return nil, fmt.Errorf("status:%d of the response to the request:%s", a.tmp.rsp.StatusCode, r.url())
	}

	a.tmp.str = a.tmp.rsp.Header.Get("Date")

	if len(a.tmp.str) > 0 {

		a.tmp.lrt, err = time.Parse(time.RFC1123, a.tmp.str)
		a.tmp.lrt = a.tmp.lrt.Truncate(time.Second)

		if err != nil {
			a.tmp.lrt = time.Now().Truncate(time.Second)
		}

	} else {
		a.tmp.lrt = time.Now().Truncate(time.Second)
	}

	a.lms.requested(a.tmp.lrt, r.wgt)
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
