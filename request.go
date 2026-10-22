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
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type Request interface {
	// Sends a request to the server and waits for a response.
	Send() ([]byte, error)

	// Add parameter to request.
	Query(key string, value string)

	// Add a header to the request.
	Header(key string, value string)
}
type request struct {
	api *api        // Pointer to parent api.
	mtd string      // Method.
	pth string      // Path.
	wgt int         // Weight.
	hdr http.Header // Headers.
	prm url.Values  // URL parameters.
	rsp *response   // Response.
}

func (r *request) init(method string, path string, weight int) {
	r.mtd = strings.ToUpper(method)
	r.pth = path
	r.wgt = weight
	r.hdr = make(http.Header)
	r.prm = make(url.Values)
}

func (r *request) encode() []byte {

	headers := []string{fmt.Sprintf("%s %s HTTP/1.1", r.mtd, r.path())}

	for k, v := range r.api.hdr {
		headers = append(headers, fmt.Sprintf("%s:%s", k, strings.Join(v, ",")))
	}

	for k, v := range r.hdr {
		headers = append(headers, fmt.Sprintf("%s:%s", k, strings.Join(v, ",")))
	}

	return []byte(strings.Join(headers, "\r\n") + "\r\n\r\n")
}

// Returns the path and parameters.
func (r *request) path() string {
	p := r.pth
	if len(r.prm) > 0 {
		p = fmt.Sprintf("/%s?%s", r.pth, r.prm.Encode())
	}
	return p
}

// Returns the full url.
func (r *request) url() string {
	return fmt.Sprintf("%s%s", r.api.host(), r.path())
}

// Add parameter to request.
func (r *request) Query(key string, value string) {
	r.prm.Add(key, value)
}

// Add a header to the request.
func (r *request) Header(key string, value string) {
	r.hdr.Add(key, value)
}

// Sends a request to the server and waits for a response.
func (r *request) Send() ([]byte, error) {
	r.api.csr <- r
	r.rsp = <-r.api.cgr
	return r.rsp.read()
}
