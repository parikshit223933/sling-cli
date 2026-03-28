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

// C-compatible struct definitions for ADBC types.
// These structs must match the C memory layout exactly (same field order, sizes, alignment).

// Arrow C Data Interface types (from adbc.h)

// ArrowSchema matches the C ArrowSchema struct layout.
type ArrowSchema struct {
	Format      *byte         // const char*
	Name        *byte         // const char*
	Metadata    *byte         // const char*
	Flags       int64         // int64_t
	NChildren   int64         // int64_t
	Children    **ArrowSchema // struct ArrowSchema**
	Dictionary  *ArrowSchema  // struct ArrowSchema*
	Release     uintptr       // void (*release)(struct ArrowSchema*)
	PrivateData uintptr       // void*
}

// ArrowArray matches the C ArrowArray struct layout.
type ArrowArray struct {
	Length      int64        // int64_t
	NullCount   int64        // int64_t
	Offset      int64        // int64_t
	NBuffers    int64        // int64_t
	NChildren   int64        // int64_t
	Buffers     *uintptr     // const void**
	Children    **ArrowArray // struct ArrowArray**
	Dictionary  *ArrowArray  // struct ArrowArray*
	Release     uintptr      // void (*release)(struct ArrowArray*)
	PrivateData uintptr      // void*
}

// ArrowArrayStream matches the C ArrowArrayStream struct layout.
type ArrowArrayStream struct {
	GetSchema    uintptr // int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out)
	GetNext      uintptr // int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out)
	GetLastError uintptr // const char* (*get_last_error)(struct ArrowArrayStream*)
	Release      uintptr // void (*release)(struct ArrowArrayStream*)
	PrivateData  uintptr // void*
}

// ADBC types (from adbc.h)

// AdbcError matches the C AdbcError struct layout (ADBC 1.1.0).
type AdbcError struct {
	Message       *byte   // char*
	VendorCode    int32   // int32_t
	SqlState      [5]byte // char[5]
	_             [3]byte // padding to align Release to pointer boundary
	Release       uintptr // void (*release)(struct AdbcError*)
	PrivateData   uintptr // void* (ADBC 1.1.0)
	PrivateDriver uintptr // struct AdbcDriver* (ADBC 1.1.0)
}

// AdbcDatabase matches the C AdbcDatabase struct layout.
type AdbcDatabase struct {
	PrivateData   uintptr // void*
	PrivateDriver uintptr // struct AdbcDriver*
}

// AdbcConnection matches the C AdbcConnection struct layout.
type AdbcConnection struct {
	PrivateData   uintptr // void*
	PrivateDriver uintptr // struct AdbcDriver*
}

// AdbcStatement matches the C AdbcStatement struct layout.
type AdbcStatement struct {
	PrivateData   uintptr // void*
	PrivateDriver uintptr // struct AdbcDriver*
}

// AdbcPartitions matches the C AdbcPartitions struct layout.
type AdbcPartitions struct {
	NumPartitions    uintptr  // size_t
	Partitions       **byte   // const uint8_t**
	PartitionLengths *uintptr // const size_t*
	PrivateData      uintptr  // void*
	Release          uintptr  // void (*release)(struct AdbcPartitions*)
}

// AdbcErrorDetail for extended error info (ADBC 1.1.0).
type AdbcErrorDetail struct {
	Key         *byte   // const char*
	Value       *byte   // const uint8_t*
	ValueLength uintptr // size_t
}

// AdbcStatusCode represents ADBC status codes.
type AdbcStatusCode uint8

// Status codes matching ADBC_STATUS_* constants.
const (
	StatusOK             AdbcStatusCode = 0
	StatusUnknown        AdbcStatusCode = 1
	StatusNotImplemented AdbcStatusCode = 2
	StatusNotFound       AdbcStatusCode = 3
	StatusAlreadyExists  AdbcStatusCode = 4
	StatusInvalidArgument AdbcStatusCode = 5
	StatusInvalidState   AdbcStatusCode = 6
	StatusInvalidData    AdbcStatusCode = 7
	StatusIntegrity      AdbcStatusCode = 8
	StatusInternal       AdbcStatusCode = 9
	StatusIO             AdbcStatusCode = 10
	StatusCancelled      AdbcStatusCode = 11
	StatusTimeout        AdbcStatusCode = 12
	StatusUnauthenticated AdbcStatusCode = 13
	StatusUnauthorized   AdbcStatusCode = 14
)

// AdbcLoadFlags for driver manager (from adbc_driver_manager.h).
type AdbcLoadFlags uint32

// Load flag constants matching ADBC_LOAD_FLAG_* constants.
const (
	LoadFlagSearchEnv          AdbcLoadFlags = 1
	LoadFlagSearchUser         AdbcLoadFlags = 2
	LoadFlagSearchSystem       AdbcLoadFlags = 4
	LoadFlagAllowRelativePaths AdbcLoadFlags = 8
	LoadFlagDefault            AdbcLoadFlags = LoadFlagSearchEnv | LoadFlagSearchUser | LoadFlagSearchSystem | LoadFlagAllowRelativePaths
)
