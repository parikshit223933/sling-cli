package database

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

func init() {
	ChunkByColumnRange = chunkByColumnRangeImpl
	ChunkByCount = chunkByCountImpl
	ChunkByExpression = chunkByExpressionImpl
	ChunkByColumn = chunkByColumnImpl
}

// parseDurationString parses chunk_size strings like "1d", "2m", "1w", "3h"
// into time.Duration. Supports d(days), w(weeks), m(months~30d), h(hours).
func parseDurationString(s string) (time.Duration, bool) {
	s = strings.TrimSpace(strings.ToLower(s))
	re := regexp.MustCompile(`^(\d+)([dwmh])$`)
	matches := re.FindStringSubmatch(s)
	if len(matches) != 3 {
		return 0, false
	}

	n, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, false
	}

	switch matches[2] {
	case "h":
		return time.Duration(n) * time.Hour, true
	case "d":
		return time.Duration(n) * 24 * time.Hour, true
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, true
	case "m":
		return time.Duration(n) * 30 * 24 * time.Hour, true
	}
	return 0, false
}

// getMinMax queries the source for the min and max values of a column,
// respecting any WHERE clause on the table and user-provided min/max overrides.
func getMinMax(conn Connection, t Table, colName string, userMin, userMax string) (minVal, maxVal string, col iop.Column, err error) {
	var sql string
	quotedCol := conn.Quote(colName)

	if t.IsQuery() {
		// Table is a SQL subquery — wrap it
		sql = g.F(
			"select min(%s) as min_val, max(%s) as max_val from (%s) as _chunk_subq",
			quotedCol, quotedCol, t.SQL,
		)
	} else {
		whereClause := ""
		if t.Where != "" {
			whereClause = " where " + t.Where
		}
		sql = g.F(
			"select min(%s) as min_val, max(%s) as max_val from %s%s",
			quotedCol, quotedCol, t.FDQN(), whereClause,
		)
	}

	data, err := conn.Query(sql)
	if err != nil {
		return "", "", col, g.Error(err, "could not get min/max for column %s", colName)
	}

	if len(data.Rows) == 0 || len(data.Rows[0]) < 2 {
		return "", "", col, nil // empty table
	}

	col = data.Columns[0]
	dbMin := cast.ToString(data.Rows[0][0])
	dbMax := cast.ToString(data.Rows[0][1])

	// Use user-provided values if set, otherwise use DB values
	minVal = lo_ternary(userMin != "", userMin, dbMin)
	maxVal = lo_ternary(userMax != "", userMax, dbMax)

	return minVal, maxVal, col, nil
}

// lo_ternary is a simple ternary helper to avoid importing lo in this file
func lo_ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// chunkByColumnRangeImpl splits a table read into chunks based on fixed-size
// ranges on the update_key column. chunk_size can be a number (row-based range)
// or a duration string like "1d", "2m" (time-based range).
func chunkByColumnRangeImpl(conn Connection, t Table, colName string, chunkSize, userMin, userMax string) ([]Chunk, error) {
	minVal, maxVal, col, err := getMinMax(conn, t, colName, userMin, userMax)
	if err != nil {
		return nil, err
	}
	if minVal == "" || maxVal == "" {
		return []Chunk{}, nil // empty table or no range
	}

	sp := iop.NewStreamProcessor()

	// Try time-based chunking first
	if dur, ok := parseDurationString(chunkSize); ok {
		minTime, err := sp.ParseTime(minVal)
		if err != nil {
			return nil, g.Error(err, "could not parse min value as time: %s", minVal)
		}
		maxTime, err := sp.ParseTime(maxVal)
		if err != nil {
			return nil, g.Error(err, "could not parse max value as time: %s", maxVal)
		}

		layout := "2006-01-02 15:04:05"
		if col.IsDate() {
			layout = "2006-01-02"
		}

		chunks := []Chunk{}
		for cursor := minTime; cursor.Before(maxTime); cursor = cursor.Add(dur) {
			end := cursor.Add(dur)
			if end.After(maxTime) {
				end = maxTime
			}
			chunks = append(chunks, Chunk{
				RangeStart: cursor.Format(layout),
				RangeEnd:   end.Format(layout),
			})
		}

		if len(chunks) == 0 {
			chunks = append(chunks, Chunk{
				RangeStart: minTime.Format(layout),
				RangeEnd:   maxTime.Format(layout),
			})
		}

		return chunks, nil
	}

	// Numeric chunking
	size := cast.ToFloat64(chunkSize)
	if size <= 0 {
		return nil, g.Error("invalid chunk_size: %s", chunkSize)
	}

	minNum := cast.ToFloat64(minVal)
	maxNum := cast.ToFloat64(maxVal)

	if col.IsDatetime() || col.IsDate() {
		// If column is date/datetime but chunk_size is numeric, treat as error
		return nil, g.Error("time-based column %s requires a duration chunk_size (e.g., '1d', '2h'), got: %s", colName, chunkSize)
	}

	chunks := []Chunk{}
	for cursor := minNum; cursor < maxNum; cursor += size {
		end := cursor + size
		if end > maxNum {
			end = maxNum
		}

		if col.IsInteger() {
			chunks = append(chunks, Chunk{
				RangeStart: cast.ToString(int64(cursor)),
				RangeEnd:   cast.ToString(int64(end)),
			})
		} else {
			chunks = append(chunks, Chunk{
				RangeStart: cast.ToString(cursor),
				RangeEnd:   cast.ToString(end),
			})
		}
	}

	if len(chunks) == 0 {
		chunks = append(chunks, Chunk{
			RangeStart: cast.ToString(minVal),
			RangeEnd:   cast.ToString(maxVal),
		})
	}

	return chunks, nil
}

// chunkByCountImpl splits a table read into N equal chunks based on the
// update_key column's min/max range.
func chunkByCountImpl(conn Connection, t Table, colName string, chunkCount int, userMin, userMax string) ([]Chunk, string, error) {
	if chunkCount <= 0 {
		return nil, "", g.Error("chunk_count must be > 0")
	}

	minVal, maxVal, col, err := getMinMax(conn, t, colName, userMin, userMax)
	if err != nil {
		return nil, "", err
	}
	if minVal == "" || maxVal == "" {
		return []Chunk{}, "", nil
	}

	sp := iop.NewStreamProcessor()

	// Time-based column
	if col.IsDatetime() || col.IsDate() {
		minTime, err := sp.ParseTime(minVal)
		if err != nil {
			return nil, "", g.Error(err, "could not parse min value as time: %s", minVal)
		}
		maxTime, err := sp.ParseTime(maxVal)
		if err != nil {
			return nil, "", g.Error(err, "could not parse max value as time: %s", maxVal)
		}

		totalDuration := maxTime.Sub(minTime)
		chunkDuration := totalDuration / time.Duration(chunkCount)
		if chunkDuration <= 0 {
			chunkDuration = time.Second
		}

		layout := "2006-01-02 15:04:05"
		if col.IsDate() {
			layout = "2006-01-02"
		}

		chunks := []Chunk{}
		for i := 0; i < chunkCount; i++ {
			start := minTime.Add(chunkDuration * time.Duration(i))
			end := minTime.Add(chunkDuration * time.Duration(i+1))
			if i == chunkCount-1 {
				end = maxTime // last chunk takes the remainder
			}
			chunks = append(chunks, Chunk{
				RangeStart: start.Format(layout),
				RangeEnd:   end.Format(layout),
			})
		}

		// Calculate chunk_size as duration string for logging
		chunkSizeStr := chunkDuration.String()

		return chunks, chunkSizeStr, nil
	}

	// Numeric column
	minNum := cast.ToFloat64(minVal)
	maxNum := cast.ToFloat64(maxVal)
	totalRange := maxNum - minNum
	chunkSizeNum := totalRange / float64(chunkCount)
	if chunkSizeNum <= 0 {
		chunkSizeNum = 1
	}

	chunks := []Chunk{}
	for i := 0; i < chunkCount; i++ {
		start := minNum + chunkSizeNum*float64(i)
		end := minNum + chunkSizeNum*float64(i+1)
		if i == chunkCount-1 {
			end = maxNum
		}

		if col.IsInteger() {
			chunks = append(chunks, Chunk{
				RangeStart: cast.ToString(int64(math.Floor(start))),
				RangeEnd:   cast.ToString(int64(math.Ceil(end))),
			})
		} else {
			chunks = append(chunks, Chunk{
				RangeStart: cast.ToString(start),
				RangeEnd:   cast.ToString(end),
			})
		}
	}

	chunkSizeStr := cast.ToString(int64(math.Ceil(chunkSizeNum)))

	return chunks, chunkSizeStr, nil
}

// chunkByExpressionImpl splits a table read using a SQL expression (typically
// hash-based) to create N chunks with WHERE clauses like "mod(hash(col), N) = i".
func chunkByExpressionImpl(conn Connection, t Table, expr string, chunkCount int) ([]Chunk, error) {
	if chunkCount <= 0 {
		return nil, g.Error("chunk_count must be > 0")
	}

	// Replace {chunk_count} placeholder in expression
	resolvedExpr := strings.ReplaceAll(expr, "{chunk_count}", cast.ToString(chunkCount))

	chunks := make([]Chunk, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunks[i] = Chunk{
			Where: g.F("(%s) = %d", resolvedExpr, i),
		}
	}

	return chunks, nil
}

// chunkByColumnImpl splits a table into multiple Table objects by partitioning
// on a column. This is a legacy function — the caller code currently uses
// ChunkByColumnRange and ChunkByCount instead.
func chunkByColumnImpl(conn Connection, table Table, colName string, parts int) ([]Table, error) {
	if parts <= 0 {
		return []Table{table}, nil
	}

	chunks, _, err := chunkByCountImpl(conn, table, colName, parts, "", "")
	if err != nil {
		return nil, err
	}

	quotedCol := conn.Quote(colName)
	tables := make([]Table, len(chunks))
	for i, chunk := range chunks {
		t := table.Clone()

		var where string
		if chunk.Where != "" {
			where = chunk.Where
		} else if chunk.RangeStart != "" || chunk.RangeEnd != "" {
			where = g.F("%s >= '%s' and %s <= '%s'",
				quotedCol, chunk.RangeStart,
				quotedCol, chunk.RangeEnd,
			)
		}

		if where != "" {
			if t.Where != "" {
				t.Where = g.F("(%s) and (%s)", t.Where, where)
			} else {
				t.Where = where
			}
		}
		tables[i] = t
	}

	return tables, nil
}
