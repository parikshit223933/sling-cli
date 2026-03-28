package iop

import (
	"encoding/json"
	"strings"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// stageTransform implements the Transform interface
// It applies a list of legacy transforms (hash_md5, etc.) to specific columns
type stageTransform struct {
	stages []transformStage
	sp     *StreamProcessor
	casted bool
}

type transformStage struct {
	colName   string
	colIndex  int
	transform TransformLegacy
	resolved  bool // whether colIndex has been resolved
}

func (st *stageTransform) Casted() bool {
	return st.casted
}

func (st *stageTransform) Evaluate(row []any) ([]any, error) {
	if st.sp == nil || st.sp.ds == nil {
		return row, nil
	}

	// Resolve column indices on first call (columns may not be available at creation time)
	for i := range st.stages {
		if !st.stages[i].resolved {
			cols := st.sp.ds.Columns
			for ci, col := range cols {
				if strings.EqualFold(col.Name, st.stages[i].colName) {
					st.stages[i].colIndex = ci
					st.stages[i].resolved = true
					break
				}
			}
		}
	}

	for _, stage := range st.stages {
		if !stage.resolved {
			continue
		}
		if stage.colIndex >= len(row) {
			continue
		}

		val := row[stage.colIndex]
		if val == nil {
			continue
		}

		// Apply the transform
		if stage.transform.FuncString != nil {
			strVal := cast.ToString(val)
			newVal, err := stage.transform.FuncString(st.sp, strVal)
			if err != nil {
				return row, g.Error(err, "transform %s failed on column %s", stage.transform.Name, stage.colName)
			}
			row[stage.colIndex] = newVal
		} else if stage.transform.Func != nil {
			newVal, err := stage.transform.Func(st.sp, val)
			if err != nil {
				return row, g.Error(err, "transform %s failed on column %s", stage.transform.Name, stage.colName)
			}
			row[stage.colIndex] = newVal
		}
	}

	return row, nil
}

func init() {
	// Override the stub functions with real implementations

	ParseStageTransforms = func(payload any) ([]map[string]string, error) {
		if payload == nil {
			return nil, nil
		}

		var stages []map[string]string

		switch v := payload.(type) {
		case []map[string]string:
			stages = v
		case []interface{}:
			for _, item := range v {
				switch m := item.(type) {
				case map[string]interface{}:
					stage := make(map[string]string)
					for k, val := range m {
						stage[k] = cast.ToString(val)
					}
					stages = append(stages, stage)
				case map[string]string:
					stages = append(stages, m)
				}
			}
		case map[string]interface{}:
			// Single stage: {col: transform, col2: transform2}
			stage := make(map[string]string)
			for k, val := range v {
				stage[k] = cast.ToString(val)
			}
			stages = append(stages, stage)
		case map[string]string:
			stages = append(stages, v)
		case string:
			// Try JSON
			if err := json.Unmarshal([]byte(v), &stages); err != nil {
				// Try as single map
				var single map[string]string
				if err2 := json.Unmarshal([]byte(v), &single); err2 != nil {
					return nil, g.Error("could not parse transforms: %s", v)
				}
				stages = append(stages, single)
			}
		default:
			// Try marshaling and unmarshaling
			data, err := json.Marshal(payload)
			if err != nil {
				return nil, g.Error(err, "could not marshal transforms")
			}
			if err := json.Unmarshal(data, &stages); err != nil {
				return nil, g.Error(err, "could not unmarshal transforms")
			}
		}

		return stages, nil
	}

	NewTransform = func(stages []map[string]string, sp *StreamProcessor) Transform {
		if len(stages) == 0 {
			return nil
		}

		st := &stageTransform{
			sp: sp,
		}

		for _, stage := range stages {
			for colName, transformName := range stage {
				transformName = strings.TrimSpace(transformName)

				// Look up the legacy transform by name
				if legacyTransform, ok := TransformsLegacyMap[transformName]; ok {
					st.stages = append(st.stages, transformStage{
						colName:   colName,
						colIndex:  -1,
						transform: legacyTransform,
					})
				} else {
					g.Warn("unknown transform '%s' for column '%s', skipping", transformName, colName)
				}
			}
		}

		if len(st.stages) == 0 {
			return nil
		}

		return st
	}
}
