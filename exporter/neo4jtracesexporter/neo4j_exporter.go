// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package neo4jtracesexporter

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/SigNoz/signoz-otel-collector/usage"
	"github.com/google/uuid"
	"go.opencensus.io/stats/view"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.16.0"
	"go.uber.org/zap"
)

// Crete new exporter.
func newExporter(cfg component.ExporterConfig, logger *zap.Logger) (*storage, error) {

	configClickHouse := cfg.(*Config)

	f := Neo4jNewFactory(configClickHouse.Migrations, configClickHouse.Datasource, configClickHouse.DockerMultiNodeCluster)

	err := f.Initialize(logger)
	if err != nil {
		return nil, err
	}
	spanWriter, err := f.CreateSpanWriter()
	if err != nil {
		return nil, err
	}

	if err := view.Register(SpansCountView, SpansCountBytesView); err != nil {
		return nil, err
	}

	storage := storage{Writer: spanWriter}

	return &storage, nil
}

type storage struct {
	Writer Writer
}

func makeJaegerProtoReferences(
	links ptrace.SpanLinkSlice,
	parentSpanID pcommon.SpanID,
	traceID pcommon.TraceID,
) ([]OtelSpanRef, error) {

	parentSpanIDSet := len([8]byte(parentSpanID)) != 0
	if !parentSpanIDSet && links.Len() == 0 {
		return nil, nil
	}

	refsCount := links.Len()
	if parentSpanIDSet {
		refsCount++
	}

	refs := make([]OtelSpanRef, 0, refsCount)

	// Put parent span ID at the first place because usually backends look for it
	// as the first CHILD_OF item in the model.SpanRef slice.
	if parentSpanIDSet {

		refs = append(refs, OtelSpanRef{
			TraceId: traceID.HexString(),
			SpanId:  parentSpanID.HexString(),
			RefType: "CHILD_OF",
		})
	}

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)

		refs = append(refs, OtelSpanRef{
			TraceId: link.TraceID().HexString(),
			SpanId:  link.SpanID().HexString(),

			// Since Jaeger RefType is not captured in internal data,
			// use SpanRefType_FOLLOWS_FROM by default.
			// SpanRefType_CHILD_OF supposed to be set only from parentSpanID.
			RefType: "FOLLOWS_FROM",
		})
	}

	return refs, nil
}

//func NewResource(resource pcommon.Resource) Resource {
//	name := "<nil-service-name>"
//	resourceType := ResourceTypeUnknown
//
//	serviceName, found := resource.Attributes().Get(conventions.AttributeServiceName)
//	if found {
//		name = serviceName.Str()
//	}
//
//	return Resource{
//		Name:       name,
//		Type:       resourceType,
//		Operations: nil,
//	}
//}

func NewResourceFromSpan(span ptrace.Span) Resource {
	name := "<nil-service-name>"
	resourceType := ResourceTypeUnknown

	dbSystem, foundDbSystem := span.Attributes().Get(conventions.AttributeDBName)
	dbName, found := span.Attributes().Get(conventions.AttributeDBName)
	if found {
		if foundDbSystem {
			name = fmt.Sprintf("%s.%s", dbSystem.Str(), dbName.Str())
		} else {
			name = dbName.Str()
		}
		resourceType = ResourceTypeStorage
		return Resource{
			Name: name,
			Type: resourceType,
		}
	}

	userAgent, found := span.Attributes().Get(conventions.AttributeHTTPUserAgent)
	if found {
		name = strings.Split(userAgent.Str(), "/")[0]
		resourceType = ResourceTypeBrowser
		return Resource{
			Name: name,
			Type: resourceType,
		}
	}

	serviceName, found := span.Attributes().Get(conventions.AttributeServiceName)
	if found {
		name = serviceName.Str()
		resourceType = ResourceTypeService
		return Resource{
			Name: name,
			Type: resourceType,
		}
	}

	return Resource{
		Name: name,
		Type: resourceType,
	}
}

func NewOperationFromSpan(span ptrace.Span) Operation {
	return Operation{
		Name:      span.Name(),
		DefinedIn: NewResourceFromSpan(span),
	}
}

func populateOtherDimensions(attributes pcommon.Map, span *Span) {

	attributes.Range(func(k string, v pcommon.Value) bool {
		if k == "http.status_code" {
			if v.Int() >= 400 {
				span.HasError = true
			}
			span.HttpCode = strconv.FormatInt(v.Int(), 10)
			span.ResponseStatusCode = span.HttpCode
		} else if k == "http.url" && span.Kind == 3 {
			value := v.Str()
			valueUrl, err := url.Parse(value)
			if err == nil {
				value = valueUrl.Hostname()
			}
			span.ExternalHttpUrl = value
		} else if k == "http.method" && span.Kind == 3 {
			span.ExternalHttpMethod = v.Str()
		} else if k == "http.url" && span.Kind != 3 {
			span.HttpUrl = v.Str()
		} else if k == "http.method" && span.Kind != 3 {
			span.HttpMethod = v.Str()
		} else if k == "http.route" {
			span.HttpRoute = v.Str()
		} else if k == "http.host" {
			span.HttpHost = v.Str()
		} else if k == "messaging.system" {
			span.MsgSystem = v.Str()
		} else if k == "messaging.operation" {
			span.MsgOperation = v.Str()
		} else if k == "component" {
			span.Component = v.Str()
		} else if k == "db.system" {
			span.DBSystem = v.Str()
		} else if k == "db.name" {
			span.DBName = v.Str()
		} else if k == "db.operation" {
			span.DBOperation = v.Str()
		} else if k == "peer.service" {
			span.PeerService = v.Str()
		} else if k == "rpc.grpc.status_code" {
			// Handle both string/int status code in GRPC spans.
			statusString, err := strconv.Atoi(v.Str())
			statusInt := v.Int()
			if err == nil && statusString != 0 {
				statusInt = int64(statusString)
			}
			if statusInt >= 2 {
				span.HasError = true
			}
			span.GRPCCode = strconv.FormatInt(statusInt, 10)
			span.ResponseStatusCode = span.GRPCCode
		} else if k == "rpc.method" {
			span.RPCMethod = v.Str()
			system, found := attributes.Get("rpc.system")
			if found && system.Str() == "grpc" {
				span.GRPCMethod = v.Str()
			}
		} else if k == "rpc.service" {
			span.RPCService = v.Str()
		} else if k == "rpc.system" {
			span.RPCSystem = v.Str()
		} else if k == "rpc.jsonrpc.error_code" {
			span.ResponseStatusCode = v.Str()
		}
		return true

	})

}

func populateEvents(events ptrace.SpanEventSlice, span *Span) {
	for i := 0; i < events.Len(); i++ {
		event := Event{}
		event.Name = events.At(i).Name()
		event.TimeUnixNano = uint64(events.At(i).Timestamp())
		event.AttributeMap = map[string]string{}
		event.IsError = false
		events.At(i).Attributes().Range(func(k string, v pcommon.Value) bool {
			event.AttributeMap[k] = v.AsString()
			return true
		})
		if event.Name == "exception" {
			event.IsError = true
			span.ErrorEvent = event
			uuidWithHyphen := uuid.New()
			uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
			span.ErrorID = uuid
			hmd5 := md5.Sum([]byte(span.ServiceName + span.ErrorEvent.AttributeMap["exception.type"] + span.ErrorEvent.AttributeMap["exception.message"]))
			span.ErrorGroupID = fmt.Sprintf("%x", hmd5)
		}
		stringEvent, _ := json.Marshal(event)
		span.Events = append(span.Events, string(stringEvent))
	}
}

func populateTraceModel(span *Span) {
	span.TraceModel.Events = span.Events
	span.TraceModel.HasError = span.HasError
}

func newStructuredSpan(otelSpan ptrace.Span, ServiceName string, resource pcommon.Resource) *Span {
	durationNano := uint64(otelSpan.EndTimestamp() - otelSpan.StartTimestamp())

	attributes := otelSpan.Attributes()
	resourceAttributes := resource.Attributes()
	tagMap := map[string]string{}
	stringTagMap := map[string]string{}
	numberTagMap := map[string]float64{}
	boolTagMap := map[string]bool{}

	attributes.Range(func(k string, v pcommon.Value) bool {
		tagMap[k] = v.AsString()
		if v.Type() == pcommon.ValueTypeDouble {
			numberTagMap[k] = v.Double()
		} else if v.Type() == pcommon.ValueTypeInt {
			numberTagMap[k] = float64(v.Int())
		} else if v.Type() == pcommon.ValueTypeBool {
			boolTagMap[k] = v.Bool()
		} else {
			stringTagMap[k] = v.AsString()
		}
		return true

	})

	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		tagMap[k] = v.AsString()
		if v.Type() == pcommon.ValueTypeDouble {
			numberTagMap[k] = v.Double()
		} else if v.Type() == pcommon.ValueTypeInt {
			numberTagMap[k] = float64(v.Int())
		} else if v.Type() == pcommon.ValueTypeBool {
			boolTagMap[k] = v.Bool()
		} else {
			stringTagMap[k] = v.AsString()
		}
		return true

	})

	references, _ := makeJaegerProtoReferences(otelSpan.Links(), otelSpan.ParentSpanID(), otelSpan.TraceID())

	tenant := usage.GetTenantNameFromResource(resource)

	var span = &Span{
		TraceId:           otelSpan.TraceID().HexString(),
		SpanId:            otelSpan.SpanID().HexString(),
		ParentSpanId:      otelSpan.ParentSpanID().HexString(),
		Name:              otelSpan.Name(),
		StartTimeUnixNano: uint64(otelSpan.StartTimestamp()),
		DurationNano:      durationNano,
		ServiceName:       ServiceName,
		Kind:              int8(otelSpan.Kind()),
		StatusCode:        int16(otelSpan.Status().Code()),
		TagMap:            tagMap,
		StringTagMap:      stringTagMap,
		NumberTagMap:      numberTagMap,
		BoolTagMap:        boolTagMap,
		HasError:          false,
		TraceModel: TraceModel{
			TraceId:           otelSpan.TraceID().HexString(),
			SpanId:            otelSpan.SpanID().HexString(),
			Name:              otelSpan.Name(),
			DurationNano:      durationNano,
			StartTimeUnixNano: uint64(otelSpan.StartTimestamp()),
			ServiceName:       ServiceName,
			Kind:              int8(otelSpan.Kind()),
			References:        references,
			TagMap:            tagMap,
			StringTagMap:      stringTagMap,
			NumberTagMap:      numberTagMap,
			BoolTagMap:        boolTagMap,
			HasError:          false,
		},
		Tenant: &tenant,
	}

	if span.StatusCode == 2 {
		span.HasError = true
	}
	populateOtherDimensions(attributes, span)
	populateEvents(otelSpan.Events(), span)
	populateTraceModel(span)

	return span
}

type Hop struct {
	IsAsynchronous bool
	IsWithError    bool
	From           Resource
	To             Operation
}

type Resource struct {
	Name string
	Type ResourceType
}

type ResourceType int

const (
	ResourceTypeUnknown ResourceType = 0
	ResourceTypeBrowser ResourceType = 2
	ResourceTypeStorage ResourceType = 3
	ResourceTypeService ResourceType = 4
)

type Operation struct {
	Name      string
	DefinedIn Resource
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	structuredSpans := make(map[pcommon.SpanID]ptrace.Span, td.ResourceSpans().Len())
	childrenSpans := make(map[pcommon.SpanID][]ptrace.Span, td.ResourceSpans().Len())

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)

		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				structuredSpans[span.SpanID()] = span
				if !span.ParentSpanID().IsEmpty() {
					childrenSlice, childSliceExists := childrenSpans[span.ParentSpanID()]
					if !childSliceExists {
						childrenSlice = make([]ptrace.Span, 2)
						childrenSpans[span.ParentSpanID()] = childrenSlice
						childrenSlice = append(childrenSlice, span)
					}
				}
			}
		}
	}

	hops := make([]Hop, 0, len(structuredSpans))

	for _, span := range structuredSpans {
		if span.Kind() == ptrace.SpanKindInternal {
			continue
		}

		parent, hasNoParent := structuredSpans[span.ParentSpanID()]
		_, hasChildrenSpans := childrenSpans[span.SpanID()]
		hasNoCorrespondingServerSpan := !hasChildrenSpans

		// a call produces two spans, one of the client side, one on the server side, the root span is usually a server call
		// while the leaf can be a client (e.g. when db is called by the terminating service in a path) - same for producer/consumers

		// if a client span with a corresponding server span in children, then continue, we will process the parent
		if (span.Kind() == ptrace.SpanKindClient || span.Kind() == ptrace.SpanKindProducer) && !hasNoCorrespondingServerSpan {
			continue
		}

		isAsyncRequest := span.Kind() == ptrace.SpanKindProducer || span.Kind() == ptrace.SpanKindConsumer
		isHasError := span.Status().Code() == 2

		hop := Hop{}
		// if no server requests, process client
		if hasNoCorrespondingServerSpan {
			// we not always process client request, it's an exception
			hop = Hop{
				IsAsynchronous: isAsyncRequest,
				IsWithError:    isHasError,
				From:           NewResourceFromSpan(span),
				To:             NewOperationFromSpan(span),
			}
		} else {
			// current span is server one, get parent one - client and define resource, operations
			clientSpan := parent
			if hasNoParent {
				clientSpan = span
			}
			serverSpan := span

			hop = Hop{
				IsAsynchronous: isAsyncRequest,
				IsWithError:    isHasError,
				From:           NewResourceFromSpan(clientSpan),
				To:             NewOperationFromSpan(serverSpan),
			}

		}

		hops = append(hops, hop)
	}

	err := s.Writer.WriteSpan(structuredSpan)
	if err != nil {
		zap.S().Error("Error in writing hops to clickhouse: ", err)
	}

	return nil
}

func (s *storage) calculateSpanHash() {

}

// Shutdown will shutdown the exporter.
func (s *storage) Shutdown(ctx context.Context) error {
	return s.Writer.Close(ctx)
}
