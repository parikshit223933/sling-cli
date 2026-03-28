package decoder

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/francoispqt/gojay"
	"github.com/spf13/cast"
)

//Unmarshaler represnets unmarshaler
type Unmarshaler interface {
	gojay.UnmarshalerJSONObject
	set(ptr interface{})
}

//newUnmarshaler represents a marshaler constructor
type newUnmarshaler func(ptr interface{}) Unmarshaler

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})

func baseUnmarshaler(sourceType string, targetType reflect.Type) (func(dec *gojay.Decoder, dest unsafe.Pointer) error, error) {
	switch sourceType {
	case "BIGNUMERIC", "BIGDECIMAL", "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = int64(i)
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(i)
				return nil
			}, nil
		case reflect.Int16, reflect.Uint16:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int16)(dest) = int16(i)
				return nil
			}, nil
		case reflect.Int8, reflect.Uint8:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int8)(dest) = int8(i)
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) (err error) {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return err
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = i
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "BYTES":
		switch targetType.Kind() {
		case reflect.Slice:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				data, err := base64.StdEncoding.DecodeString(text)
				if err != nil || !ok {
					return err
				}
				*(*[]byte)(dest) = data
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				data, err := base64.StdEncoding.DecodeString(text)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = data
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "STRING":
		switch targetType.Kind() {
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = text
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "NUMERIC", "DECIMAL", "FLOAT64", "FLOAT":
		switch targetType.Kind() {
		case reflect.Float32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float32)(dest) = float32(f)
				return nil
			}, nil
		case reflect.Float64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float64)(dest) = float64(f)
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = f
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "TIME", "TIMESTAMP", "DATE", "DATETIME":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = ts.UnixNano()
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(ts.Unix())
				return nil
			}, nil

		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = ts
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = ts
				return nil
			}, nil
		case reflect.Struct:
			if targetType.ConvertibleTo(timeType) {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					if err != nil || !ok {
						return err
					}
					*(*time.Time)(dest) = *ts
					return nil
				}, nil
			}
		case reflect.Ptr:
			if targetType.ConvertibleTo(timePtrType) {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					*(**time.Time)(dest) = ts
					return nil
				}, nil
			}
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}
	case "BOOLEAN":
		switch targetType.Kind() {
		case reflect.Bool:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*bool)(dest) = b
				return nil
			}, nil
		case reflect.Int, reflect.Int8, reflect.Uint8, reflect.Uint:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				v := int8(0)
				if b {
					v = 1
				}
				*(*int8)(dest) = v
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = b
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = b
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
		}
	}
	return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
}

func decodeTime(dec *gojay.Decoder) (*time.Time, bool, error) {
	var valueFlt *float64
	err := dec.Float64Null(&valueFlt)
	if err == nil {
		f, ok, err := decodeFloat(dec)
		if err != nil || !ok {
			return nil, false, err
		}
		timestamp := int64(f*1000000) * int64(time.Microsecond)
		ts := time.Unix(0, timestamp)
		return &ts, true, nil
	}

	var valueStr *string
	err = dec.StringNull(&valueStr)
	if err != nil || valueStr == nil {
		return nil, false, err
	}
	if valueStr == nil {
		return nil, false, nil
	}

	t, err := parseTime(*valueStr)
	if err != nil {
		return nil, false, err
	}
	return &t, true, nil
}

func decodeInt(dec *gojay.Decoder) (int, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 0, false, err
	}
	i, err := strconv.Atoi(*value)
	if err != nil {
		return 0, false, err
	}
	return i, true, nil
}

func decodeBool(dec *gojay.Decoder) (bool, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return false, false, err
	}
	b, err := strconv.ParseBool(*value)
	if err != nil {
		return false, false, err
	}
	return b, true, nil
}

func decodeFloat(dec *gojay.Decoder) (float64, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 0, false, err
	}
	if value == nil {
		return 0, false, nil
	}
	i, err := strconv.ParseFloat(*value, 64)
	if err != nil {
		fmt.Printf("%#v", getCallerStack(1))
		return 0, false, err
	}
	return i, true, nil
}

func decodeString(dec *gojay.Decoder) (string, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return "", false, err
	}
	return *value, true, nil
}

func getCallerStack(levelsUp int) []string {
	callerArr := []string{}
	for {
		pc, file, no, ok := runtime.Caller(levelsUp)
		if !ok {
			break
		}
		details := runtime.FuncForPC(pc)
		funcNameArr := strings.Split(details.Name(), ".")
		funcName := funcNameArr[len(funcNameArr)-1]
		fileArr := strings.Split(file, "/")
		callStr := fmt.Sprintf("%s:%d %s", fileArr[len(fileArr)-1], no, funcName)
		if strings.Contains(callStr, "goexit") {
			break
		}
		callerArr = append(callerArr, callStr)
		levelsUp++
	}
	return callerArr
}

var dateLayoutCache = ""
var dateLayouts = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02T15:04:05.000Z",
	"02-Jan-06",
	"02-Jan-06 15:04:05",
	"02-Jan-06 03:04:05 PM",
	"02-Jan-06 03.04.05.000000 PM",
	"2006-01-02T15:04:05-0700",
	time.RFC3339,
	"2006-01-02T15:04:05",  // iso8601 without timezone
	"2006-01-02T15:04:05Z", // iso8601 with timezone
	time.RFC1123Z,
	time.RFC1123,
	time.RFC822Z,
	time.RFC822,
	time.RFC850,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	"2006-01-02 15:04:05.999999999 -0700 MST", // Time.String()
	"02 Jan 2006",
	"2006-01-02T15:04:05-0700", // RFC3339 without timezone hh:mm colon
	"2006-01-02 15:04:05 -07:00",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05Z07:00", // RFC3339 without T
	"2006-01-02 15:04:05Z0700",  // RFC3339 without T or timezone hh:mm colon
	"2006-01-02 15:04:05 MST",
	time.Kitchen,
	time.Stamp,
	time.StampMilli,
	time.StampMicro,
	time.StampNano,
	"1/2/06",
	"01/02/06",
	"1/2/2006",
	"01/02/2006",
	"01/02/2006 15:04",
	"01/02/2006 15:04:05",
	"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
	"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006/01/02 15:04:05",
}

func parseTime(i interface{}) (t time.Time, err error) {
	s := cast.ToString(i)

	// date layouts to try out
	for _, layout := range dateLayouts {
		// use cache to decrease parsing computation next iteration
		if dateLayoutCache != "" {
			t, err = time.Parse(dateLayoutCache, s)
			if err == nil {
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			dateLayoutCache = layout
			return
		}
	}
	return
}
