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
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"net/url"
	"time"

	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/spf13/viper"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

// Factory implements storage.Factory for Clickhouse backend.
type Factory struct {
	logger     *zap.Logger
	Options    *Options
	db         neo4j.SessionWithContext
	datasource string
	makeWriter writerMaker
}

// Writer writes hops to storage.
type Writer interface {
	WriteHop(span *Hop) error
	Close(ctx context.Context) error
}

type writerMaker func(logger *zap.Logger, db neo4j.SessionWithContext, traceDatabase string, spansTable string, indexTable string, errorTable string, encoding Encoding, delay time.Duration, size int) (Writer, error)

var (
	writeLatencyMillis = stats.Int64("exporter_db_write_latency", "Time taken (in millis) for exporter to write batch", "ms")
	exporterKey        = tag.MustNewKey("exporter")
	tableKey           = tag.MustNewKey("table")
)

// Neo4jNewFactory creates a new Factory.
func Neo4jNewFactory(migrations string, datasource string, dockerMultiNodeCluster bool) *Factory {
	//writeLatencyDistribution := view.Distribution(100, 250, 500, 750, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000)

	//writeLatencyView := &view.View{
	//	Name:        "exporter_db_write_latency",
	//	Measure:     writeLatencyMillis,
	//	Description: writeLatencyMillis.Description(),
	//	TagKeys:     []tag.Key{exporterKey, tableKey},
	//	Aggregation: writeLatencyDistribution,
	//}

	//view.Register(writeLatencyView)
	return &Factory{
		Options: NewOptions(migrations, datasource, dockerMultiNodeCluster, primaryNamespace),
		// makeReader: func(db *clickhouse.Conn, operationsTable, indexTable, spansTable string) (spanstore.Reader, error) {
		// 	return store.NewTraceReader(db, operationsTable, indexTable, spansTable), nil
		// },
		makeWriter: func(logger *zap.Logger, db neo4j.SessionWithContext, traceDatabase string, spansTable string, indexTable string, errorTable string, encoding Encoding, delay time.Duration, size int) (Writer, error) {
			return NewHopWriter(logger, db, traceDatabase, spansTable, indexTable, errorTable, encoding, delay, size), nil
		},
	}
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(logger *zap.Logger) error {
	f.logger = logger

	db, err := f.connect(f.Options.getPrimary())
	if err != nil {
		return fmt.Errorf("error connecting to primary db: %v", err)
	}

	f.db = db

	return nil
}

func buildNeo4jMigrateURL(datasource string, cluster string) (string, error) {
	// return fmt.Sprintf("clickhouse://localhost:9000?database=default&x-multi-statement=true"), nil
	var clickhouseUrl string
	database := "signoz_traces"
	parsedURL, err := url.Parse(datasource)
	if err != nil {
		return "", err
	}
	host := parsedURL.Host
	if host == "" {
		return "", fmt.Errorf("Unable to parse host")

	}
	paramMap, err := url.ParseQuery(parsedURL.RawQuery)
	if err != nil {
		return "", err
	}
	username := paramMap["username"]
	password := paramMap["password"]

	if len(username) > 0 && len(password) > 0 {
		clickhouseUrl = fmt.Sprintf("clickhouse://%s:%s@%s/%s?x-multi-statement=true&x-cluster-name=%s&x-migrations-table=schema_migrations&x-migrations-table-engine=MergeTree", username[0], password[0], host, database, cluster)
	} else {
		clickhouseUrl = fmt.Sprintf("clickhouse://%s/%s?x-multi-statement=true&x-cluster-name=%s&x-migrations-table=schema_migrations&x-migrations-table-engine=MergeTree", host, database, cluster)
	}
	return clickhouseUrl, nil
}

func (f *Factory) connect(cfg *namespaceConfig) (neo4j.SessionWithContext, error) {
	if cfg.Encoding != EncodingJSON && cfg.Encoding != EncodingProto {
		return nil, fmt.Errorf("unknown encoding %q, supported: %q, %q", cfg.Encoding, EncodingJSON, EncodingProto)
	}

	return cfg.Connector(cfg)
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (Writer, error) {
	cfg := f.Options.getPrimary()
	return f.makeWriter(f.logger, f.db, cfg.TraceDatabase, cfg.SpansTable, cfg.IndexTable, cfg.ErrorTable, cfg.Encoding, cfg.WriteBatchDelay, cfg.WriteBatchSize)
}

// Close Implements io.Closer and closes the underlying storage
func (f *Factory) Close(ctx context.Context) error {
	if f.db != nil {
		err := f.db.Close(ctx)
		if err != nil {
			return err
		}

		f.db = nil
	}

	return nil
}
