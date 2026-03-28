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
	"fmt"
	"os"
	"runtime"
	"sync"
)

var (
	driverManagerLib uintptr
	loadOnce         sync.Once
	loadErr          error
)

// loadDriverManagerLibrary loads the ADBC driver manager shared library.
// It uses sync.Once to ensure the library is only loaded once per process.
// The library path can be overridden via the ADBC_DRIVER_MANAGER_LIB environment variable.
func loadDriverManagerLibrary() (uintptr, error) {
	loadOnce.Do(func() {
		var libName string
		switch runtime.GOOS {
		case "darwin":
			libName = "libadbc_driver_manager.dylib"
		case "linux":
			libName = "libadbc_driver_manager.so"
		case "windows":
			libName = "adbc_driver_manager.dll"
		default:
			loadErr = fmt.Errorf("unsupported OS: %s", runtime.GOOS)
			return
		}

		// Allow override via environment variable
		if override := os.Getenv("ADBC_DRIVER_MANAGER_LIB"); override != "" {
			libName = override
		}

		var lib uintptr
		lib, loadErr = loadLibrary(libName)
		if loadErr != nil {
			loadErr = fmt.Errorf("failed to load ADBC driver manager library %q: %w. You can specify the exact path with env var ADBC_DRIVER_MANAGER_LIB", libName, loadErr)
			return
		}

		driverManagerLib = lib
		loadErr = registerFunctions(lib)
	})

	return driverManagerLib, loadErr
}
