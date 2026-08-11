package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/stretchr/testify/assert"
)

// The ClickHouse delete_insert merge, in the shape emitted by
// core/dbio/templates/clickhouse.yaml.
const chMergeSQL = "DELETE FROM `lsq`.`prospectactivity_base`\n" +
	"WHERE (prospect_activity_id) IN (\n" +
	"  SELECT prospect_activity_id FROM `lsq`.`prospectactivity_base_tmp`\n" +
	");\n" +
	"INSERT INTO `lsq`.`prospectactivity_base` (prospect_activity_id)\n" +
	"SELECT prospect_activity_id FROM `lsq`.`prospectactivity_base_tmp` src"

// recordingConn stubs Connection so ExecMultiContext's `conn.Self().ExecContext`
// can be observed. Only ExecContext is implemented; the embedded nil Connection
// makes any other call panic loudly rather than quietly return a zero value and
// hide a broken test.
type recordingConn struct {
	Connection
	typ        dbio.Type
	executed   []string
	failOnCall int // 1-based index of the call that should fail; 0 = never fail
}

// GetType is reached through BaseConn.GetTemplateValue, which resolves
// `variable.multi_exec_delay` via conn.Self().GetType().
func (c *recordingConn) GetType() dbio.Type { return c.typ }

func (c *recordingConn) ExecContext(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	c.executed = append(c.executed, q)
	if c.failOnCall > 0 && len(c.executed) == c.failOnCall {
		// Shape of the real production failure: the lightweight DELETE's
		// background mutation died with MEMORY_LIMIT_EXCEEDED, so the DELETE
		// came back as ExceptionBeforeStart with code 341.
		return nil, g.Error("Code: 341. DB::Exception: Exception happened during execution of mutation")
	}
	return Result{rowsAffected: 1}, nil
}

func stubbedConn(t dbio.Type, stub *recordingConn) *BaseConn {
	stub.typ = t
	conn := &BaseConn{Type: t}
	var self Connection = stub
	conn.instance = &self
	return conn
}

// The regression test for the duplicate-rows bug.
//
// ClickHouse has no transactional DML, so if the DELETE half of the merge fails
// and the INSERT half runs anyway, the batch is written on top of the rows the
// DELETE was meant to remove - and the duplicates are permanent, because
// incremental_buffer has already shifted the next run's window past them.
// ~15,500 rows accumulated this way across lsq.prospectactivity_base and
// lsq.prospectactivity_extensionbase.
func TestExecMultiAbandonsRemainingStatementsOnClickhouse(t *testing.T) {
	stub := &recordingConn{failOnCall: 1} // the DELETE fails
	conn := stubbedConn(dbio.TypeDbClickhouse, stub)

	_, err := conn.ExecMultiContext(context.Background(), chMergeSQL)

	assert.Error(t, err, "a failed DELETE must still surface as an error")
	assert.Len(t, stub.executed, 1, "the INSERT must NOT run after the DELETE failed")
	assert.Contains(t, stub.executed[0], "DELETE FROM")
}

// Every other dialect keeps the original collect-and-continue behaviour. There
// the merge runs inside a real transaction, so the first error aborts it and the
// statements that follow cannot persist; changing this would alter behaviour
// across all the non-ClickHouse pipelines for no benefit.
func TestExecMultiContinuesPastFailureOnPostgres(t *testing.T) {
	stub := &recordingConn{failOnCall: 1}
	conn := stubbedConn(dbio.TypeDbPostgres, stub)

	_, err := conn.ExecMultiContext(context.Background(), chMergeSQL)

	assert.Error(t, err)
	assert.Len(t, stub.executed, 2, "non-ClickHouse dialects keep going, as before")
}

// The happy path must be untouched: both statements run and rowsAffected sums.
func TestExecMultiRunsEveryStatementOnSuccess(t *testing.T) {
	stub := &recordingConn{}
	conn := stubbedConn(dbio.TypeDbClickhouse, stub)

	res, err := conn.ExecMultiContext(context.Background(), chMergeSQL)

	assert.NoError(t, err)
	assert.Len(t, stub.executed, 2)
	assert.Contains(t, stub.executed[0], "DELETE FROM")
	assert.Contains(t, stub.executed[1], "INSERT INTO")

	ra, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, ra)
}

// A failure on the INSERT must still be reported, not swallowed just because the
// DELETE before it succeeded.
func TestExecMultiReportsLaterStatementFailure(t *testing.T) {
	stub := &recordingConn{failOnCall: 2}
	conn := stubbedConn(dbio.TypeDbClickhouse, stub)

	_, err := conn.ExecMultiContext(context.Background(), chMergeSQL)

	assert.Error(t, err)
	assert.Len(t, stub.executed, 2)
}

func TestAbandonMultiStatementOnError(t *testing.T) {
	assert.True(t, abandonMultiStatementOnError(dbio.TypeDbClickhouse),
		"ClickHouse has no transaction to roll back a statement that ran after a failure")

	for _, typ := range []dbio.Type{
		dbio.TypeDbPostgres,
		dbio.TypeDbMySQL,
		dbio.TypeDbSQLServer,
		dbio.TypeDbSnowflake,
		dbio.TypeDbBigQuery,
	} {
		assert.False(t, abandonMultiStatementOnError(typ),
			"%s keeps the original behaviour", typ)
	}
}

// NOTE: BaseTransaction.ExecMultiContext carries the same guard and is the path
// production actually takes (MergeWithStrategy uses tx.ExecMultiContext whenever a
// transaction is open, and ClickhouseConn.Merge always opens one). It cannot be
// unit-tested here because BaseTransaction.ExecContext goes straight to a real
// *sqlx.Tx and no SQL mock is vendored, so it is covered end-to-end by the Docker
// integration harness instead (chdup-test/run.sh), which runs a real ClickHouse
// 26.2.5.45 - the exact production version - and asserts duplicates appear on the
// unfixed binary and do not on the fixed one.
