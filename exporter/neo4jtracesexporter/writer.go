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
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Encoding string

const (
	// EncodingJSON is used for hops encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for hops encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)

// HopWriter for writing hops to ClickHouse
type HopWriter struct {
	logger        *zap.Logger
	db            neo4j.SessionWithContext
	traceDatabase string
	indexTable    string
	errorTable    string
	spansTable    string
	encoding      Encoding
	delay         time.Duration
	size          int
	hops          chan *Hop
	finish        chan bool
	done          sync.WaitGroup
}

// NewHopWriter returns a HopWriter for the database
func NewHopWriter(logger *zap.Logger, db neo4j.SessionWithContext, traceDatabase string, spansTable string, indexTable string, errorTable string, encoding Encoding, delay time.Duration, size int) *HopWriter {
	//if err := view.Register(SpansCountView, SpansCountBytesView); err != nil {
	//	return nil
	//}
	writer := &HopWriter{
		logger:        logger,
		db:            db,
		traceDatabase: traceDatabase,
		indexTable:    indexTable,
		errorTable:    errorTable,
		spansTable:    spansTable,
		encoding:      encoding,
		delay:         delay,
		size:          size,
		hops:          make(chan *Hop, size),
		finish:        make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *HopWriter) backgroundWriter() {
	batch := make([]*Hop, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.hops:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of hops", zap.Error(err))
			}

			batch = make([]*Hop, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *HopWriter) writeBatch(batch []*Hop) error {

	//if w.spansTable != "" {
	//	if err := w.writeModelBatch(batch); err != nil {
	//		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	//		w.logger.Error("Could not write a batch of spans to model table: ", zap.Any("batch", logBatch), zap.Error(err))
	//		return err
	//	}
	//}
	//if w.indexTable != "" {
	if err := w.writeHops(batch); err != nil {
		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
		w.logger.Error("Could not write a batch of spans to index table: ", zap.Any("batch", logBatch), zap.Error(err))
		return err
	}
	//}
	//if w.errorTable != "" {
	//	if err := w.writeErrorBatch(batch); err != nil {
	//		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	//		w.logger.Error("Could not write a batch of spans to error table: ", zap.Any("batch", logBatch), zap.Error(err))
	//		return err
	//	}
	//}

	return nil
}

func (w *HopWriter) writeHops(batchHops []*Hop) error {
	ctx := context.Background()

	//statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.indexTable))
	//if err != nil {
	//	logBatch := batchHops[:int(math.Min(10, float64(len(batchHops))))]
	//	w.logger.Error("Could not prepare batch for index table: ", zap.Any("batch", logBatch), zap.Error(err))
	//	return err
	//}

	for _, hop := range batchHops {
		_, err := w.db.Run(ctx, `
MERGE (operation:Operation {name: $toOperationName, resourceName: $toResourceName})
ON CREATE
    SET
        operation.createdAt =  timestamp(),
        operation.lastUsedAt = timestamp()
ON MATCH
    SET operation.lastUsedAt = timestamp()

WITH operation

OPTIONAL MATCH (r:Resource) 
    WHERE $toResourceName =~ "(?i).*" + r.name + ".*" 
		OR r.name =~ "(?i).*" + $toResourceName + ".*"
WITH CASE r IS NULL
        WHEN true THEN $toResourceName
        ELSE r.name
     END as resourceName, operation
MERGE (toResource:Resource {name: resourceName})
ON CREATE
    SET
        toResource.createdAt =  timestamp(),
        toResource.lastUsedAt = timestamp(),
        toResource.type = $toResourceType
ON MATCH
    SET toResource.lastUsedAt = timestamp()

WITH operation, toResource

MERGE
    (toResource)-[provides:Provides]->(operation)
ON CREATE
    SET
        provides.createdAt =  timestamp(),
        provides.lastUsedAt = timestamp()
ON MATCH
    SET provides.lastUsedAt = timestamp()

WITH operation, toResource

OPTIONAL MATCH (r:Resource) 
    WHERE $fromResourceName =~ "(?i).*" + r.name + ".*" 
		OR r.name =~ "(?i).*" + $fromResourceName + ".*"
WITH CASE r IS NULL
        WHEN true THEN $fromResourceName
        ELSE r.name
     END as resourceName, operation, toResource
MERGE (fromResource:Resource {name: resourceName})
ON CREATE
    SET
        fromResource.createdAt =  timestamp(),
        fromResource.lastUsedAt = timestamp(),
        fromResource.type = $fromResourceType
ON MATCH
    SET fromResource.lastUsedAt = timestamp()

WITH operation, toResource, fromResource

MERGE
    (fromResource)-[c:Calls]->(operation)
ON CREATE
    SET
        c.createdAt =  timestamp(),
        c.lastUsedAt = timestamp(),
        c.callsCount = 1,
        c.errorsCount = CASE $isErrorHop
                            WHEN true THEN 1
                            ELSE 0
                        END,
        c.type = CASE $isAsyncHop
                    WHEN true THEN "async"
                    ELSE "sync"
                 END
ON MATCH
    SET c.lastUsedAt = timestamp(),
    c.callsCount = c.callsCount + 1,
    c.errorsCount = c.errorsCount + CASE $isErrorHop 
                        WHEN true THEN 1
                        ELSE 0
                    END
					`, map[string]any{
			"fromResourceName": hop.From.Name,
			"fromResourceType": hop.From.Type,
			"toResourceName":   hop.To.DefinedIn.Name,
			"toResourceType":   hop.To.DefinedIn.Type,
			"toOperationName":  hop.To.Name,
			"isAsyncHop":       hop.IsAsynchronous,
			"isErrorHop":       hop.IsWithError,
		})

		if err != nil {
			w.logger.Error("Could not upsert hop", zap.Object("hop", hop), zap.Error(err))
			return err
		}
	}

	//start := time.Now()
	//
	//err = statement.Send()
	//
	//ctx, _ = tag.New(ctx,
	//	tag.Upsert(exporterKey, string(component.DataTypeTraces)),
	//	tag.Upsert(tableKey, w.indexTable),
	//)
	//stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
	//return err
	return nil
}

//func (w *HopWriter) writeErrorBatch(batchSpans []*Span) error {
//
//	ctx := context.Background()
//	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.errorTable))
//	if err != nil {
//		logBatch := batchSpans[:int(math.Min(10, float64(len(batchSpans))))]
//		w.logger.Error("Could not prepare batch for error table: ", zap.Any("batch", logBatch), zap.Error(err))
//		return err
//	}
//
//	for _, span := range batchSpans {
//		if span.ErrorEvent.Name == "" {
//			continue
//		}
//		err = statement.Append(
//			time.Unix(0, int64(span.ErrorEvent.TimeUnixNano)),
//			span.ErrorID,
//			span.ErrorGroupID,
//			span.TraceId,
//			span.SpanId,
//			span.ServiceName,
//			span.ErrorEvent.AttributeMap["exception.type"],
//			span.ErrorEvent.AttributeMap["exception.message"],
//			span.ErrorEvent.AttributeMap["exception.stacktrace"],
//			stringToBool(span.ErrorEvent.AttributeMap["exception.escaped"]),
//		)
//		if err != nil {
//			w.logger.Error("Could not append span to batch: ", zap.Object("span", span), zap.Error(err))
//			return err
//		}
//	}
//
//	start := time.Now()
//
//	err = statement.Send()
//
//	ctx, _ = tag.New(ctx,
//		tag.Upsert(exporterKey, string(component.DataTypeTraces)),
//		tag.Upsert(tableKey, w.errorTable),
//	)
//	stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
//	return err
//}

func stringToBool(s string) bool {
	if strings.ToLower(s) == "true" {
		return true
	}
	return false
}

//func (w *HopWriter) writeModelBatch(batchSpans []*Span) error {
//	ctx := context.Background()
//	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.spansTable))
//	if err != nil {
//		logBatch := batchSpans[:int(math.Min(10, float64(len(batchSpans))))]
//		w.logger.Error("Could not prepare batch for model table: ", zap.Any("batch", logBatch), zap.Error(err))
//		return err
//	}
//
//	metrics := map[string]usage.Metric{}
//	for _, span := range batchSpans {
//		var serialized []byte
//
//		serialized, err = json.Marshal(span.TraceModel)
//
//		if err != nil {
//			return err
//		}
//
//		err = statement.Append(time.Unix(0, int64(span.StartTimeUnixNano)), span.TraceId, string(serialized))
//		if err != nil {
//			w.logger.Error("Could not append span to batch: ", zap.Object("span", span), zap.Error(err))
//			return err
//		}
//
//		usage.AddMetric(metrics, *span.Tenant, 1, int64(len(serialized)))
//	}
//	start := time.Now()
//
//	err = statement.Send()
//	ctx, _ = tag.New(ctx,
//		tag.Upsert(exporterKey, string(component.DataTypeTraces)),
//		tag.Upsert(tableKey, w.spansTable),
//	)
//	stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
//	if err != nil {
//		return err
//	}
//	for k, v := range metrics {
//		stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(usage.TagTenantKey, k)}, ExporterSigNozSentSpans.M(int64(v.Count)), ExporterSigNozSentSpansBytes.M(int64(v.Size)))
//	}
//
//	return nil
//}

// WriteHop writes the encoded span
func (w *HopWriter) WriteHop(hop *Hop) error {
	w.hops <- hop
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *HopWriter) Close(_ context.Context) error {
	w.finish <- true
	w.done.Wait()
	return nil
}
