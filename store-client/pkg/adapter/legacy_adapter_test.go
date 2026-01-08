// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package adapter

import (
	"testing"

	"github.com/nvidia/nvsentinel/store-client/pkg/config"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyDatabaseConfigAdapter_ConnectionPoolMethods verifies that
// the legacy adapter implements the new connection pool methods with correct defaults.
func TestLegacyDatabaseConfigAdapter_ConnectionPoolMethods(t *testing.T) {
	dsConfig := &datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			Host:     "mongodb://localhost:27017",
			Database: "testdb",
		},
	}

	adapter := NewLegacyDatabaseConfigAdapter(dsConfig)

	// Verify adapter implements DatabaseConfig interface
	var _ config.DatabaseConfig = adapter

	// Verify connection pool defaults match expected values
	assert.Equal(t, "", adapter.GetAppName(), "AppName should be empty for legacy adapter")
	assert.Equal(t, uint64(3), adapter.GetMaxPoolSize(), "MaxPoolSize default should be 3")
	assert.Equal(t, uint64(1), adapter.GetMinPoolSize(), "MinPoolSize default should be 1")
	assert.Equal(t, 300, adapter.GetMaxConnIdleTimeSeconds(), "MaxConnIdleTime should be 300s (5 min)")
}

// TestConvertDataStoreConfigToLegacy verifies the conversion functions work correctly.
func TestConvertDataStoreConfigToLegacy(t *testing.T) {
	dsConfig := &datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			Host:     "mongodb://localhost:27017",
			Database: "testdb",
		},
	}

	// Test without cert path
	dbConfig := ConvertDataStoreConfigToLegacy(dsConfig)
	require.NotNil(t, dbConfig)
	assert.Equal(t, "testdb", dbConfig.GetDatabaseName())
	assert.Equal(t, uint64(3), dbConfig.GetMaxPoolSize())

	// Test with cert path
	dbConfigWithCert := ConvertDataStoreConfigToLegacyWithCertPath(dsConfig, "/custom/cert/path")
	require.NotNil(t, dbConfigWithCert)
	assert.Equal(t, uint64(3), dbConfigWithCert.GetMaxPoolSize())
}
