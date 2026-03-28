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

import "github.com/ebitengine/purego"

// Database functions (ADBC 1.0.0)
var (
	adbcDatabaseNew       func(*AdbcDatabase, *AdbcError) AdbcStatusCode
	adbcDatabaseInit      func(*AdbcDatabase, *AdbcError) AdbcStatusCode
	adbcDatabaseRelease   func(*AdbcDatabase, *AdbcError) AdbcStatusCode
	adbcDatabaseSetOption func(*AdbcDatabase, *byte, *byte, *AdbcError) AdbcStatusCode
)

// Database functions (ADBC 1.1.0)
var (
	adbcDatabaseGetOption       func(*AdbcDatabase, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcDatabaseGetOptionBytes  func(*AdbcDatabase, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcDatabaseGetOptionDouble func(*AdbcDatabase, *byte, *float64, *AdbcError) AdbcStatusCode
	adbcDatabaseGetOptionInt    func(*AdbcDatabase, *byte, *int64, *AdbcError) AdbcStatusCode
	adbcDatabaseSetOptionBytes  func(*AdbcDatabase, *byte, *byte, uintptr, *AdbcError) AdbcStatusCode
	adbcDatabaseSetOptionDouble func(*AdbcDatabase, *byte, float64, *AdbcError) AdbcStatusCode
	adbcDatabaseSetOptionInt    func(*AdbcDatabase, *byte, int64, *AdbcError) AdbcStatusCode
)

// Driver Manager specific functions
var (
	adbcDriverManagerDatabaseSetLoadFlags func(*AdbcDatabase, AdbcLoadFlags, *AdbcError) AdbcStatusCode
)

// Connection functions (ADBC 1.0.0)
var (
	adbcConnectionNew            func(*AdbcConnection, *AdbcError) AdbcStatusCode
	adbcConnectionInit           func(*AdbcConnection, *AdbcDatabase, *AdbcError) AdbcStatusCode
	adbcConnectionRelease        func(*AdbcConnection, *AdbcError) AdbcStatusCode
	adbcConnectionSetOption      func(*AdbcConnection, *byte, *byte, *AdbcError) AdbcStatusCode
	adbcConnectionGetInfo        func(*AdbcConnection, *uint32, uintptr, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcConnectionGetObjects     func(*AdbcConnection, int32, *byte, *byte, *byte, **byte, *byte, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcConnectionGetTableSchema func(*AdbcConnection, *byte, *byte, *byte, *ArrowSchema, *AdbcError) AdbcStatusCode
	adbcConnectionGetTableTypes  func(*AdbcConnection, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcConnectionCommit         func(*AdbcConnection, *AdbcError) AdbcStatusCode
	adbcConnectionRollback       func(*AdbcConnection, *AdbcError) AdbcStatusCode
	adbcConnectionReadPartition  func(*AdbcConnection, *byte, uintptr, *ArrowArrayStream, *AdbcError) AdbcStatusCode
)

// Connection functions (ADBC 1.1.0)
var (
	adbcConnectionCancel            func(*AdbcConnection, *AdbcError) AdbcStatusCode
	adbcConnectionGetOption         func(*AdbcConnection, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcConnectionGetOptionBytes    func(*AdbcConnection, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcConnectionGetOptionDouble   func(*AdbcConnection, *byte, *float64, *AdbcError) AdbcStatusCode
	adbcConnectionGetOptionInt      func(*AdbcConnection, *byte, *int64, *AdbcError) AdbcStatusCode
	adbcConnectionGetStatistics     func(*AdbcConnection, *byte, *byte, *byte, byte, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcConnectionGetStatisticNames func(*AdbcConnection, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcConnectionSetOptionBytes    func(*AdbcConnection, *byte, *byte, uintptr, *AdbcError) AdbcStatusCode
	adbcConnectionSetOptionDouble   func(*AdbcConnection, *byte, float64, *AdbcError) AdbcStatusCode
	adbcConnectionSetOptionInt      func(*AdbcConnection, *byte, int64, *AdbcError) AdbcStatusCode
)

// Statement functions (ADBC 1.0.0)
var (
	adbcStatementNew                func(*AdbcConnection, *AdbcStatement, *AdbcError) AdbcStatusCode
	adbcStatementRelease            func(*AdbcStatement, *AdbcError) AdbcStatusCode
	adbcStatementSetOption          func(*AdbcStatement, *byte, *byte, *AdbcError) AdbcStatusCode
	adbcStatementSetSqlQuery        func(*AdbcStatement, *byte, *AdbcError) AdbcStatusCode
	adbcStatementSetSubstraitPlan   func(*AdbcStatement, *byte, uintptr, *AdbcError) AdbcStatusCode
	adbcStatementPrepare            func(*AdbcStatement, *AdbcError) AdbcStatusCode
	adbcStatementExecuteQuery       func(*AdbcStatement, *ArrowArrayStream, *int64, *AdbcError) AdbcStatusCode
	adbcStatementExecutePartitions  func(*AdbcStatement, *ArrowSchema, *AdbcPartitions, *int64, *AdbcError) AdbcStatusCode
	adbcStatementBind               func(*AdbcStatement, *ArrowArray, *ArrowSchema, *AdbcError) AdbcStatusCode
	adbcStatementBindStream         func(*AdbcStatement, *ArrowArrayStream, *AdbcError) AdbcStatusCode
	adbcStatementGetParameterSchema func(*AdbcStatement, *ArrowSchema, *AdbcError) AdbcStatusCode
)

// Statement functions (ADBC 1.1.0)
var (
	adbcStatementCancel          func(*AdbcStatement, *AdbcError) AdbcStatusCode
	adbcStatementExecuteSchema   func(*AdbcStatement, *ArrowSchema, *AdbcError) AdbcStatusCode
	adbcStatementGetOption       func(*AdbcStatement, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcStatementGetOptionBytes  func(*AdbcStatement, *byte, *byte, *uintptr, *AdbcError) AdbcStatusCode
	adbcStatementGetOptionDouble func(*AdbcStatement, *byte, *float64, *AdbcError) AdbcStatusCode
	adbcStatementGetOptionInt    func(*AdbcStatement, *byte, *int64, *AdbcError) AdbcStatusCode
	adbcStatementSetOptionBytes  func(*AdbcStatement, *byte, *byte, uintptr, *AdbcError) AdbcStatusCode
	adbcStatementSetOptionDouble func(*AdbcStatement, *byte, float64, *AdbcError) AdbcStatusCode
	adbcStatementSetOptionInt    func(*AdbcStatement, *byte, int64, *AdbcError) AdbcStatusCode
)

// Error functions (ADBC 1.1.0)
var (
	adbcErrorGetDetailCount  func(*AdbcError) int32
	adbcErrorGetDetail       func(*AdbcError, int32) AdbcErrorDetail
	adbcErrorFromArrayStream func(*ArrowArrayStream, *AdbcStatusCode) *AdbcError
)

// registerFunctions registers all ADBC function bindings with purego.
// This is called once during library loading.
func registerFunctions(lib uintptr) error {
	// Database functions (ADBC 1.0.0)
	purego.RegisterLibFunc(&adbcDatabaseNew, lib, "AdbcDatabaseNew")
	purego.RegisterLibFunc(&adbcDatabaseInit, lib, "AdbcDatabaseInit")
	purego.RegisterLibFunc(&adbcDatabaseRelease, lib, "AdbcDatabaseRelease")
	purego.RegisterLibFunc(&adbcDatabaseSetOption, lib, "AdbcDatabaseSetOption")

	// Database functions (ADBC 1.1.0)
	purego.RegisterLibFunc(&adbcDatabaseGetOption, lib, "AdbcDatabaseGetOption")
	purego.RegisterLibFunc(&adbcDatabaseGetOptionBytes, lib, "AdbcDatabaseGetOptionBytes")
	purego.RegisterLibFunc(&adbcDatabaseGetOptionDouble, lib, "AdbcDatabaseGetOptionDouble")
	purego.RegisterLibFunc(&adbcDatabaseGetOptionInt, lib, "AdbcDatabaseGetOptionInt")
	purego.RegisterLibFunc(&adbcDatabaseSetOptionBytes, lib, "AdbcDatabaseSetOptionBytes")
	purego.RegisterLibFunc(&adbcDatabaseSetOptionDouble, lib, "AdbcDatabaseSetOptionDouble")
	purego.RegisterLibFunc(&adbcDatabaseSetOptionInt, lib, "AdbcDatabaseSetOptionInt")

	// Driver Manager extensions
	purego.RegisterLibFunc(&adbcDriverManagerDatabaseSetLoadFlags, lib, "AdbcDriverManagerDatabaseSetLoadFlags")

	// Connection functions (ADBC 1.0.0)
	purego.RegisterLibFunc(&adbcConnectionNew, lib, "AdbcConnectionNew")
	purego.RegisterLibFunc(&adbcConnectionInit, lib, "AdbcConnectionInit")
	purego.RegisterLibFunc(&adbcConnectionRelease, lib, "AdbcConnectionRelease")
	purego.RegisterLibFunc(&adbcConnectionSetOption, lib, "AdbcConnectionSetOption")
	purego.RegisterLibFunc(&adbcConnectionGetInfo, lib, "AdbcConnectionGetInfo")
	purego.RegisterLibFunc(&adbcConnectionGetObjects, lib, "AdbcConnectionGetObjects")
	purego.RegisterLibFunc(&adbcConnectionGetTableSchema, lib, "AdbcConnectionGetTableSchema")
	purego.RegisterLibFunc(&adbcConnectionGetTableTypes, lib, "AdbcConnectionGetTableTypes")
	purego.RegisterLibFunc(&adbcConnectionCommit, lib, "AdbcConnectionCommit")
	purego.RegisterLibFunc(&adbcConnectionRollback, lib, "AdbcConnectionRollback")
	purego.RegisterLibFunc(&adbcConnectionReadPartition, lib, "AdbcConnectionReadPartition")

	// Connection functions (ADBC 1.1.0)
	purego.RegisterLibFunc(&adbcConnectionCancel, lib, "AdbcConnectionCancel")
	purego.RegisterLibFunc(&adbcConnectionGetOption, lib, "AdbcConnectionGetOption")
	purego.RegisterLibFunc(&adbcConnectionGetOptionBytes, lib, "AdbcConnectionGetOptionBytes")
	purego.RegisterLibFunc(&adbcConnectionGetOptionDouble, lib, "AdbcConnectionGetOptionDouble")
	purego.RegisterLibFunc(&adbcConnectionGetOptionInt, lib, "AdbcConnectionGetOptionInt")
	purego.RegisterLibFunc(&adbcConnectionGetStatistics, lib, "AdbcConnectionGetStatistics")
	purego.RegisterLibFunc(&adbcConnectionGetStatisticNames, lib, "AdbcConnectionGetStatisticNames")
	purego.RegisterLibFunc(&adbcConnectionSetOptionBytes, lib, "AdbcConnectionSetOptionBytes")
	purego.RegisterLibFunc(&adbcConnectionSetOptionDouble, lib, "AdbcConnectionSetOptionDouble")
	purego.RegisterLibFunc(&adbcConnectionSetOptionInt, lib, "AdbcConnectionSetOptionInt")

	// Statement functions (ADBC 1.0.0)
	purego.RegisterLibFunc(&adbcStatementNew, lib, "AdbcStatementNew")
	purego.RegisterLibFunc(&adbcStatementRelease, lib, "AdbcStatementRelease")
	purego.RegisterLibFunc(&adbcStatementSetOption, lib, "AdbcStatementSetOption")
	purego.RegisterLibFunc(&adbcStatementSetSqlQuery, lib, "AdbcStatementSetSqlQuery")
	purego.RegisterLibFunc(&adbcStatementSetSubstraitPlan, lib, "AdbcStatementSetSubstraitPlan")
	purego.RegisterLibFunc(&adbcStatementPrepare, lib, "AdbcStatementPrepare")
	purego.RegisterLibFunc(&adbcStatementExecuteQuery, lib, "AdbcStatementExecuteQuery")
	purego.RegisterLibFunc(&adbcStatementExecutePartitions, lib, "AdbcStatementExecutePartitions")
	purego.RegisterLibFunc(&adbcStatementBind, lib, "AdbcStatementBind")
	purego.RegisterLibFunc(&adbcStatementBindStream, lib, "AdbcStatementBindStream")
	purego.RegisterLibFunc(&adbcStatementGetParameterSchema, lib, "AdbcStatementGetParameterSchema")

	// Statement functions (ADBC 1.1.0)
	purego.RegisterLibFunc(&adbcStatementCancel, lib, "AdbcStatementCancel")
	purego.RegisterLibFunc(&adbcStatementExecuteSchema, lib, "AdbcStatementExecuteSchema")
	purego.RegisterLibFunc(&adbcStatementGetOption, lib, "AdbcStatementGetOption")
	purego.RegisterLibFunc(&adbcStatementGetOptionBytes, lib, "AdbcStatementGetOptionBytes")
	purego.RegisterLibFunc(&adbcStatementGetOptionDouble, lib, "AdbcStatementGetOptionDouble")
	purego.RegisterLibFunc(&adbcStatementGetOptionInt, lib, "AdbcStatementGetOptionInt")
	purego.RegisterLibFunc(&adbcStatementSetOptionBytes, lib, "AdbcStatementSetOptionBytes")
	purego.RegisterLibFunc(&adbcStatementSetOptionDouble, lib, "AdbcStatementSetOptionDouble")
	purego.RegisterLibFunc(&adbcStatementSetOptionInt, lib, "AdbcStatementSetOptionInt")

	// Error functions (ADBC 1.1.0)
	purego.RegisterLibFunc(&adbcErrorGetDetailCount, lib, "AdbcErrorGetDetailCount")
	purego.RegisterLibFunc(&adbcErrorGetDetail, lib, "AdbcErrorGetDetail")
	purego.RegisterLibFunc(&adbcErrorFromArrayStream, lib, "AdbcErrorFromArrayStream")

	return nil
}
