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
	"context"
	"strconv"
	"sync"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const (
	LoadFlagsSearchEnv = 1 << iota
	LoadFlagsSearchPath
	LoadFlagsSearchSystem
	LoadFlagsAllowRelativePaths

	LoadFlagsDefault = LoadFlagsSearchEnv | LoadFlagsSearchPath | LoadFlagsSearchSystem | LoadFlagsAllowRelativePaths
	// LoadFlagsOptionKey is the key to use for an option to set specific
	// load flags for the database to decide where to look for driver manifests.
	LoadFlagsOptionKey = "load_flags"
)

// option holds a key-value pair with null-terminated byte slices.
type option struct {
	key, val []byte
}

func convOptions(incoming map[string]string, existing map[string]option) {
	for k, v := range incoming {
		o := option{
			key: append([]byte(k), 0),
			val: append([]byte(v), 0),
		}
		existing[k] = o
	}
}

type Driver struct{}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d Driver) NewDatabaseWithContext(_ context.Context, opts map[string]string) (adbc.Database, error) {
	// Ensure library is loaded
	if _, err := loadDriverManagerLibrary(); err != nil {
		return nil, err
	}

	dbOptions := make(map[string]option)
	convOptions(opts, dbOptions)

	db := &Database{
		options: make(map[string]option),
	}

	var adbcErr AdbcError
	var adbcDb AdbcDatabase

	if code := adbcDatabaseNew(&adbcDb, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	if code := adbcDriverManagerDatabaseSetLoadFlags(&adbcDb, AdbcLoadFlags(LoadFlagsDefault), &adbcErr); code != StatusOK {
		errOut := toAdbcError(code, &adbcErr)
		adbcDatabaseRelease(&adbcDb, &adbcErr)
		return nil, errOut
	}

	for k, o := range dbOptions {
		switch k {
		case LoadFlagsOptionKey:
			f, errOut := strconv.Atoi(string(o.val[:len(o.val)-1])) // exclude null terminator
			if errOut != nil {
				adbcDatabaseRelease(&adbcDb, &adbcErr)
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "invalid load flags value: " + string(o.val[:len(o.val)-1]),
				}
			}

			if code := adbcDriverManagerDatabaseSetLoadFlags(&adbcDb, AdbcLoadFlags(f), &adbcErr); code != StatusOK {
				errOut := toAdbcError(code, &adbcErr)
				adbcDatabaseRelease(&adbcDb, &adbcErr)
				return nil, errOut
			}
		default:
			if code := adbcDatabaseSetOption(&adbcDb, &o.key[0], &o.val[0], &adbcErr); code != StatusOK {
				errOut := toAdbcError(code, &adbcErr)
				adbcDatabaseRelease(&adbcDb, &adbcErr)
				return nil, errOut
			}
		}
	}

	if code := adbcDatabaseInit(&adbcDb, &adbcErr); code != StatusOK {
		errOut := toAdbcError(code, &adbcErr)
		adbcDatabaseRelease(&adbcDb, &adbcErr)
		return nil, errOut
	}

	db.db = &adbcDb
	return db, nil
}

type Database struct {
	options map[string]option
	db      *AdbcDatabase

	mu     sync.Mutex // protects following fields
	closed bool
}

func (d *Database) SetOptions(options map[string]string) error {
	if d.options == nil {
		d.options = make(map[string]option)
	}

	for k, v := range options {
		o := option{
			key: append([]byte(k), 0),
			val: append([]byte(v), 0),
		}
		d.options[k] = o
	}
	return nil
}

func (d *Database) Open(context.Context) (adbc.Connection, error) {
	var adbcErr AdbcError
	var c AdbcConnection

	if code := adbcConnectionNew(&c, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	for _, o := range d.options {
		if code := adbcConnectionSetOption(&c, &o.key[0], &o.val[0], &adbcErr); code != StatusOK {
			errOut := toAdbcError(code, &adbcErr)
			adbcConnectionRelease(&c, &adbcErr)
			return nil, errOut
		}
	}

	if code := adbcConnectionInit(&c, d.db, &adbcErr); code != StatusOK {
		errOut := toAdbcError(code, &adbcErr)
		adbcConnectionRelease(&c, &adbcErr)
		return nil, errOut
	}

	return &cnxn{conn: &c}, nil
}

func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	if d.db != nil {
		var adbcErr AdbcError
		code := adbcDatabaseRelease(d.db, &adbcErr)
		if code != StatusOK {
			return toAdbcError(code, &adbcErr)
		}
	}

	return nil
}

func getRdr(out *ArrowArrayStream) (array.RecordReader, error) {
	rdr, err := cdata.ImportCRecordReader((*cdata.CArrowArrayStream)(unsafe.Pointer(out)), nil)
	if err != nil {
		return nil, err
	}
	return rdr.(array.RecordReader), nil
}

func getSchema(out *ArrowSchema) (*arrow.Schema, error) {
	// Maybe: ImportCArrowSchema should perform this check?
	if out.Format == nil {
		return nil, nil
	}

	return cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(out)))
}

type cnxn struct {
	conn *AdbcConnection
}

func (c *cnxn) GetInfo(_ context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	var (
		out   ArrowArrayStream
		adbcErr AdbcError
		codes *uint32
	)
	if len(infoCodes) > 0 {
		codes = (*uint32)(unsafe.Pointer(&infoCodes[0]))
	}

	if code := adbcConnectionGetInfo(c.conn, codes, uintptr(len(infoCodes)), &out, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	return getRdr(&out)
}

func (c *cnxn) GetObjects(_ context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	var (
		out         ArrowArrayStream
		adbcErr     AdbcError
		catalogBuf  []byte
		dbSchemaBuf []byte
		tableNameBuf []byte
		columnNameBuf []byte
		tableTypePtrs []*byte
	)

	var catalogPtr, dbSchemaPtr, tableNamePtr, columnNamePtr *byte
	var tableTypePtr **byte

	if catalog != nil {
		catalogBuf = append([]byte(*catalog), 0)
		catalogPtr = &catalogBuf[0]
	}

	if dbSchema != nil {
		dbSchemaBuf = append([]byte(*dbSchema), 0)
		dbSchemaPtr = &dbSchemaBuf[0]
	}

	if tableName != nil {
		tableNameBuf = append([]byte(*tableName), 0)
		tableNamePtr = &tableNameBuf[0]
	}

	if columnName != nil {
		columnNameBuf = append([]byte(*columnName), 0)
		columnNamePtr = &columnNameBuf[0]
	}

	// Build null-terminated array of null-terminated strings
	tableTypeBuffers := make([][]byte, len(tableType)+1)
	if len(tableType) > 0 {
		tableTypePtrs = make([]*byte, len(tableType)+1)
		for i, tt := range tableType {
			tableTypeBuffers[i] = append([]byte(tt), 0)
			tableTypePtrs[i] = &tableTypeBuffers[i][0]
		}
		tableTypePtrs[len(tableType)] = nil // null terminator
		tableTypePtr = &tableTypePtrs[0]
	}

	if code := adbcConnectionGetObjects(c.conn, int32(depth), catalogPtr, dbSchemaPtr, tableNamePtr, tableTypePtr, columnNamePtr, &out, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}
	return getRdr(&out)
}

func (c *cnxn) GetTableSchema(_ context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	var (
		schema       ArrowSchema
		adbcErr      AdbcError
		catalogBuf   []byte
		dbSchemaBuf  []byte
		tableNameBuf []byte
	)

	var catalogPtr, dbSchemaPtr *byte

	if catalog != nil {
		catalogBuf = append([]byte(*catalog), 0)
		catalogPtr = &catalogBuf[0]
	}

	if dbSchema != nil {
		dbSchemaBuf = append([]byte(*dbSchema), 0)
		dbSchemaPtr = &dbSchemaBuf[0]
	}

	tableNameBuf = append([]byte(tableName), 0)
	tableNamePtr := &tableNameBuf[0]

	if code := adbcConnectionGetTableSchema(c.conn, catalogPtr, dbSchemaPtr, tableNamePtr, &schema, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	return getSchema(&schema)
}

func (c *cnxn) GetTableTypes(context.Context) (array.RecordReader, error) {
	var (
		out     ArrowArrayStream
		adbcErr AdbcError
	)

	if code := adbcConnectionGetTableTypes(c.conn, &out, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}
	return getRdr(&out)
}

func (c *cnxn) Commit(context.Context) error {
	var adbcErr AdbcError

	if code := adbcConnectionCommit(c.conn, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}

	return nil
}

func (c *cnxn) Rollback(context.Context) error {
	var adbcErr AdbcError

	if code := adbcConnectionRollback(c.conn, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}

	return nil
}

func (c *cnxn) NewStatement() (adbc.Statement, error) {
	var st AdbcStatement
	var adbcErr AdbcError
	if code := adbcStatementNew(c.conn, &st, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	return &stmt{st: &st}, nil
}

func (c *cnxn) Close() error {
	if c.conn == nil {
		return nil
	}
	var adbcErr AdbcError
	if code := adbcConnectionRelease(c.conn, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	c.conn = nil
	return nil
}

func (c *cnxn) ReadPartition(_ context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) SetOption(key, value string) error {
	keyBuf := append([]byte(key), 0)
	valueBuf := append([]byte(value), 0)

	var adbcErr AdbcError
	if code := adbcConnectionSetOption(c.conn, &keyBuf[0], &valueBuf[0], &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

type stmt struct {
	st *AdbcStatement
}

func (s *stmt) Close() error {
	if s.st == nil {
		return nil
	}
	var adbcErr AdbcError
	if code := adbcStatementRelease(s.st, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	s.st = nil
	return nil
}

func (s *stmt) SetOption(key, val string) error {
	keyBuf := append([]byte(key), 0)
	valBuf := append([]byte(val), 0)

	var adbcErr AdbcError
	if code := adbcStatementSetOption(s.st, &keyBuf[0], &valBuf[0], &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

func (s *stmt) SetSqlQuery(query string) error {
	var adbcErr AdbcError
	queryBuf := append([]byte(query), 0)

	if code := adbcStatementSetSqlQuery(s.st, &queryBuf[0], &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

func (s *stmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	var (
		out      ArrowArrayStream
		affected int64
		adbcErr  AdbcError
	)
	code := adbcStatementExecuteQuery(s.st, &out, &affected, &adbcErr)
	if code != StatusOK {
		return nil, 0, toAdbcError(code, &adbcErr)
	}

	rdr, goerr := getRdr(&out)
	if goerr != nil {
		return nil, affected, goerr
	}
	return rdr, affected, nil
}

func (s *stmt) ExecuteUpdate(context.Context) (int64, error) {
	var (
		nrows   int64
		adbcErr AdbcError
	)
	if code := adbcStatementExecuteQuery(s.st, nil, &nrows, &adbcErr); code != StatusOK {
		return -1, toAdbcError(code, &adbcErr)
	}
	return nrows, nil
}

func (s *stmt) Prepare(context.Context) error {
	var adbcErr AdbcError
	if code := adbcStatementPrepare(s.st, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

func (s *stmt) SetSubstraitPlan(plan []byte) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *stmt) Bind(_ context.Context, values arrow.RecordBatch) error {
	arr := allocArrowArray()
	schema := allocArrowSchema()

	cdArr := (*cdata.CArrowArray)(unsafe.Pointer(arr))
	cdSchema := (*cdata.CArrowSchema)(unsafe.Pointer(schema))

	cdata.ExportArrowRecordBatch(values, cdArr, cdSchema)
	defer func() {
		cdata.ReleaseCArrowArray(cdArr)
		cdata.ReleaseCArrowSchema(cdSchema)
	}()

	var adbcErr AdbcError
	code := adbcStatementBind(s.st, arr, schema, &adbcErr)
	if code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

func (s *stmt) BindStream(_ context.Context, stream array.RecordReader) error {
	arrStream := allocArrowArrayStream()
	cdArrStream := (*cdata.CArrowArrayStream)(unsafe.Pointer(arrStream))

	cdata.ExportRecordReader(stream, cdArrStream)

	var adbcErr AdbcError
	if code := adbcStatementBindStream(s.st, arrStream, &adbcErr); code != StatusOK {
		return toAdbcError(code, &adbcErr)
	}
	return nil
}

func (s *stmt) GetParameterSchema() (*arrow.Schema, error) {
	var (
		schema  ArrowSchema
		adbcErr AdbcError
	)

	if code := adbcStatementGetParameterSchema(s.st, &schema, &adbcErr); code != StatusOK {
		return nil, toAdbcError(code, &adbcErr)
	}

	return getSchema(&schema)
}

func (s *stmt) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, &adbc.Error{Code: adbc.StatusNotImplemented}
}
