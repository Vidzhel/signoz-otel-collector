// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package neo4jtracesexporter

import (
	"context"
	"flag"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"net/url"
	"time"

	"github.com/spf13/viper"
)

const (
	defaultDatasource               string        = "tcp://127.0.0.1:9000/?database=signoz_traces"
	defaultTraceDatabase            string        = "signoz_traces"
	defaultMigrations               string        = "/migrations"
	defaultOperationsTable          string        = "distributed_signoz_operations"
	defaultIndexTable               string        = "distributed_signoz_index_v2"
	localIndexTable                 string        = "signoz_index_v2"
	defaultErrorTable               string        = "distributed_signoz_error_index_v2"
	defaultSpansTable               string        = "distributed_signoz_spans"
	defaultDurationSortTable        string        = "durationSort"
	defaultDurationSortMVTable      string        = "durationSortMV"
	defaultClusterName              string        = "cluster"
	defaultDependencyGraphTable     string        = "dependency_graph_minutes"
	defaultDependencyGraphServiceMV string        = "dependency_graph_minutes_service_calls_mv"
	defaultDependencyGraphDbMV      string        = "dependency_graph_minutes_db_calls_mv"
	DependencyGraphMessagingMV      string        = "dependency_graph_minutes_messaging_calls_mv"
	defaultWriteBatchDelay          time.Duration = 2 * time.Second
	defaultWriteBatchSize           int           = 100000
	defaultEncoding                 Encoding      = EncodingJSON
)

const (
	suffixEnabled         = ".enabled"
	suffixDatasource      = ".datasource"
	suffixTraceDatabase   = ".trace-database"
	suffixMigrations      = ".migrations"
	suffixOperationsTable = ".operations-table"
	suffixIndexTable      = ".index-table"
	suffixSpansTable      = ".spans-table"
	suffixWriteBatchDelay = ".write-batch-delay"
	suffixWriteBatchSize  = ".write-batch-size"
	suffixEncoding        = ".encoding"
)

// NamespaceConfig is Clickhouse's internal configuration data
type namespaceConfig struct {
	namespace                  string
	Enabled                    bool
	Datasource                 string
	Migrations                 string
	TraceDatabase              string
	OperationsTable            string
	IndexTable                 string
	LocalIndexTable            string
	SpansTable                 string
	ErrorTable                 string
	Cluster                    string
	DurationSortTable          string
	DurationSortMVTable        string
	DependencyGraphServiceMV   string
	DependencyGraphDbMV        string
	DependencyGraphMessagingMV string
	DependencyGraphTable       string
	DockerMultiNodeCluster     bool
	WriteBatchDelay            time.Duration
	WriteBatchSize             int
	Encoding                   Encoding
	Connector                  Connector
}

// Connecto defines how to connect to the database
type Connector func(cfg *namespaceConfig) (neo4j.SessionWithContext, error)

func defaultConnector(cfg *namespaceConfig) (neo4j.SessionWithContext, error) {
	ctx := context.Background()
	dsnURL, err := url.Parse(cfg.Datasource)
	cleanUrl := url.URL{
		Scheme: dsnURL.Scheme,
		Host:   dsnURL.Host,
	}
	connectionUrl := cleanUrl.String()
	var auth neo4j.AuthToken
	if dsnURL.Query().Get("username") != "" {
		auth = neo4j.BasicAuth(dsnURL.Query().Get("username"), dsnURL.Query().Get("password"), "")
	}
	driver, err := neo4j.NewDriverWithContext(connectionUrl, auth)
	if err != nil {
		return nil, err
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		return nil, err
	}

	db := driver.NewSession(ctx, neo4j.SessionConfig{})

	return db, nil
}

// Options store storage plugin related configs
type Options struct {
	primary *namespaceConfig

	others map[string]*namespaceConfig
}

// NewOptions creates a new Options struct.
func NewOptions(migrations string, datasource string, dockerMultiNodeCluster bool, primaryNamespace string, otherNamespaces ...string) *Options {

	if datasource == "" {
		datasource = defaultDatasource
	}
	if migrations == "" {
		migrations = defaultMigrations
	}

	options := &Options{
		primary: &namespaceConfig{
			namespace:                  primaryNamespace,
			Enabled:                    true,
			Datasource:                 datasource,
			Migrations:                 migrations,
			TraceDatabase:              defaultTraceDatabase,
			OperationsTable:            defaultOperationsTable,
			IndexTable:                 defaultIndexTable,
			LocalIndexTable:            localIndexTable,
			ErrorTable:                 defaultErrorTable,
			SpansTable:                 defaultSpansTable,
			DurationSortTable:          defaultDurationSortTable,
			DurationSortMVTable:        defaultDurationSortMVTable,
			Cluster:                    defaultClusterName,
			DependencyGraphTable:       defaultDependencyGraphTable,
			DependencyGraphServiceMV:   defaultDependencyGraphServiceMV,
			DependencyGraphDbMV:        defaultDependencyGraphDbMV,
			DependencyGraphMessagingMV: DependencyGraphMessagingMV,
			DockerMultiNodeCluster:     dockerMultiNodeCluster,
			WriteBatchDelay:            defaultWriteBatchDelay,
			WriteBatchSize:             defaultWriteBatchSize,
			Encoding:                   defaultEncoding,
			Connector:                  defaultConnector,
		},
		others: make(map[string]*namespaceConfig, len(otherNamespaces)),
	}

	for _, namespace := range otherNamespaces {
		options.others[namespace] = &namespaceConfig{namespace: namespace}
	}

	return options
}

// AddFlags adds flags for Options
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	addFlags(flagSet, opt.primary)
	for _, cfg := range opt.others {
		addFlags(flagSet, cfg)
	}
}

func addFlags(flagSet *flag.FlagSet, nsConfig *namespaceConfig) {
	flagSet.String(
		nsConfig.namespace+suffixDatasource,
		nsConfig.Datasource,
		"Clickhouse datasource string.",
	)

	flagSet.String(
		nsConfig.namespace+suffixSpansTable,
		nsConfig.SpansTable,
		"Clickhouse hops table name.",
	)

	flagSet.Duration(
		nsConfig.namespace+suffixWriteBatchDelay,
		nsConfig.WriteBatchDelay,
		"A duration after which hops are flushed to Clickhouse",
	)

	flagSet.Int(
		nsConfig.namespace+suffixWriteBatchSize,
		nsConfig.WriteBatchSize,
		"A number of hops buffered before they are flushed to Clickhouse",
	)

	flagSet.String(
		nsConfig.namespace+suffixEncoding,
		string(nsConfig.Encoding),
		"Encoding to store hops (json allows out of band queries, protobuf is more compact)",
	)
}

// InitFromViper initializes Options with properties from viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	initFromViper(opt.primary, v)
	for _, cfg := range opt.others {
		initFromViper(cfg, v)
	}
}

func initFromViper(cfg *namespaceConfig, v *viper.Viper) {
	cfg.Enabled = v.GetBool(cfg.namespace + suffixEnabled)
	cfg.Datasource = v.GetString(cfg.namespace + suffixDatasource)
	cfg.TraceDatabase = v.GetString(cfg.namespace + suffixTraceDatabase)
	cfg.IndexTable = v.GetString(cfg.namespace + suffixIndexTable)
	cfg.SpansTable = v.GetString(cfg.namespace + suffixSpansTable)
	cfg.OperationsTable = v.GetString(cfg.namespace + suffixOperationsTable)
	cfg.WriteBatchDelay = v.GetDuration(cfg.namespace + suffixWriteBatchDelay)
	cfg.WriteBatchSize = v.GetInt(cfg.namespace + suffixWriteBatchSize)
	cfg.Encoding = Encoding(v.GetString(cfg.namespace + suffixEncoding))
}

// GetPrimary returns the primary namespace configuration
func (opt *Options) getPrimary() *namespaceConfig {
	return opt.primary
}
