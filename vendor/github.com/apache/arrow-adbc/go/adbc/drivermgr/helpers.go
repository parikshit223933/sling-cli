// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package drivermgr

import (
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/ebitengine/purego"
)

// goString converts a null-terminated C string to Go string.
func goString(cstr *byte) string {
	if cstr == nil {
		return ""
	}
	var length int
	for ptr := cstr; *ptr != 0; ptr = (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + 1)) {
		length++
	}
	return string(unsafe.Slice(cstr, length))
}

// cString converts a Go string to a null-terminated byte slice.
// The caller is responsible for keeping the returned slice alive.
func cString(s string) *byte {
	if s == "" {
		return nil
	}
	b := make([]byte, len(s)+1)
	copy(b, s)
	b[len(s)] = 0
	return &b[0]
}

// cStringFree is a no-op since Go manages memory, but kept for clarity.
func cStringFree(_ *byte) {
	// Go GC handles this
}

// toAdbcError converts a C AdbcError to a Go adbc.Error.
func toAdbcError(code AdbcStatusCode, e *AdbcError) error {
	if e == nil || e.Release == 0 {
		return adbc.Error{
			Code: adbc.Status(code),
			Msg:  "[drivermgr] unknown error",
		}
	}

	err := adbc.Error{
		Code:       adbc.Status(code),
		VendorCode: e.VendorCode,
		Msg:        goString(e.Message),
	}
	copy(err.SqlState[:], e.SqlState[:])

	// Call release callback if present
	if e.Release != 0 {
		releaseErr(e)
	}

	return err
}

// releaseErr calls the error's release callback via purego.
func releaseErr(e *AdbcError) {
	if e.Release == 0 {
		return
	}
	// Call the release function pointer
	purego.SyscallN(e.Release, uintptr(unsafe.Pointer(e)))
	e.Release = 0
}

// allocArrowArray allocates a zeroed ArrowArray.
func allocArrowArray() *ArrowArray {
	arr := new(ArrowArray)
	*arr = ArrowArray{} // zero initialize
	return arr
}

// allocArrowSchema allocates a zeroed ArrowSchema.
func allocArrowSchema() *ArrowSchema {
	schema := new(ArrowSchema)
	*schema = ArrowSchema{} // zero initialize
	return schema
}

// allocArrowArrayStream allocates a zeroed ArrowArrayStream.
func allocArrowArrayStream() *ArrowArrayStream {
	stream := new(ArrowArrayStream)
	*stream = ArrowArrayStream{} // zero initialize
	return stream
}

// allocAdbcError allocates a zeroed AdbcError.
func allocAdbcError() *AdbcError {
	e := new(AdbcError)
	*e = AdbcError{} // zero initialize
	return e
}
