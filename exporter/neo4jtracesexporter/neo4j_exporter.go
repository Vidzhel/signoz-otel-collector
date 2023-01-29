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
	"go.uber.org/zap/zapcore"
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

	storage := storage{Writer: spanWriter, Logger: logger}

	return &storage, nil
}

type storage struct {
	Writer Writer
	Logger *zap.Logger
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

func (e Hop) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddBool("isAsynchronous", e.IsAsynchronous)
	enc.AddBool("isWithError", e.IsWithError)
	err := enc.AddObject("From", e.From)
	if err != nil {
		return err
	}
	return enc.AddObject("To", e.To)
}

type Resource struct {
	Name string
	Type ResourceType
}

func (e Resource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", e.Name)
	enc.AddInt("type", int(e.Type))
	return nil
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

type SpanAndResource struct {
	Span     *ptrace.Span
	Resource *pcommon.Resource
}

func (e Operation) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", e.Name)
	return enc.AddObject("definedIn", e.DefinedIn)
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	structuredSpans := make(map[pcommon.SpanID]SpanAndResource, td.ResourceSpans().Len())
	childrenSpans := make(map[pcommon.SpanID][]SpanAndResource, td.ResourceSpans().Len())

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resource := rs.Resource()

		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				spanAndResource := SpanAndResource{
					Span:     &span,
					Resource: &resource,
				}
				structuredSpans[span.SpanID()] = spanAndResource
				if !span.ParentSpanID().IsEmpty() {
					childrenSlice, childSliceExists := childrenSpans[span.ParentSpanID()]
					if !childSliceExists {
						childrenSlice = make([]SpanAndResource, 2)
						childrenSpans[span.ParentSpanID()] = childrenSlice
						childrenSlice = append(childrenSlice, spanAndResource)
					}
				}
			}
		}
	}

	for _, spanAndResource := range structuredSpans {
		span := spanAndResource.Span
		if span.Kind() != ptrace.SpanKindClient && span.Kind() != ptrace.SpanKindServer && span.Kind() != ptrace.SpanKindProducer && span.Kind() != ptrace.SpanKindConsumer {
			s.Logger.Info(fmt.Sprintf("skip spanKind %v", span.Kind()))
			continue
		}

		parentAndResource, hasParent := structuredSpans[span.ParentSpanID()]
		_, hasChildrenSpans := childrenSpans[span.SpanID()]
		hasNoCorrespondingServerSpan := !hasChildrenSpans

		// a call produces two spans, one of the client side, one on the server side, the root span is usually a server call
		// while the leaf can be a client (e.g. when db is called by the terminating service in a path) - same for producer/consumers

		// if a client span with a corresponding server span in children, then continue, we will process the parent
		if (span.Kind() == ptrace.SpanKindClient || span.Kind() == ptrace.SpanKindProducer) && !hasNoCorrespondingServerSpan {
			continue
		}

		isAsyncRequest := span.Kind() == ptrace.SpanKindProducer || span.Kind() == ptrace.SpanKindConsumer
		isHasError := extractIsError(*span)

		hop := Hop{}
		if hasNoCorrespondingServerSpan {
			s.Logger.Info(fmt.Sprintf("No children (server) span, current: %v", span.SpanID()))
			// a client/producer span may not have corresponding server/consumer handler if the part is not
			// instrumented (usually databases are not) or is outside of this system's scope, for situations like that
			hop = Hop{
				IsAsynchronous: isAsyncRequest,
				IsWithError:    isHasError,
				From:           s.newResource(*spanAndResource.Resource),
				To:             s.inferToOperationFromClientSpan(*spanAndResource.Span),
			}
		} else if !hasParent {
			s.Logger.Info(fmt.Sprintf("No parent (client) span, current: %v, parent: %v", span.SpanID(), span.ParentSpanID()))
			// usually indicates the root span which can be service or browser
			hop = Hop{
				IsAsynchronous: isAsyncRequest,
				IsWithError:    isHasError,
				From:           s.inferFromResourceFromServerSpan(*spanAndResource.Span),
				To:             s.newOperationFromSpanAndResource(spanAndResource),
			}
		} else {
			hop = Hop{
				IsAsynchronous: isAsyncRequest,
				IsWithError:    isHasError,
				From:           s.newResource(*parentAndResource.Resource),
				To:             s.newOperationFromSpanAndResource(spanAndResource),
			}
		}

		err := s.Writer.WriteHop(&hop)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Error in writing hops to neo4j: %v", err))
		}
	}

	return nil
}

func extractIsError(span ptrace.Span) bool {
	spanCode := span.Status().Code()
	if spanCode != ptrace.StatusCodeUnset {
		return spanCode == ptrace.StatusCodeError
	}

	httpCode, found := span.Attributes().Get(conventions.AttributeRPCGRPCStatusCode)
	if found && httpCode.Int() >= 400 {
		return true
	}

	rpcCode, found := span.Attributes().Get(conventions.AttributeRPCGRPCStatusCode)
	if found {
		statusString, err := strconv.Atoi(rpcCode.Str())
		statusInt := rpcCode.Int()
		if err == nil && statusString != 0 {
			statusInt = int64(statusString)
		}
		if statusInt >= 2 {
			return true
		}
	}

	_, found = span.Attributes().Get(conventions.AttributeRPCJsonrpcErrorCode)
	if found {
		return true
	}

	return false
}

func (s *storage) newResource(resource pcommon.Resource) Resource {
	serviceName, found := resource.Attributes().Get(conventions.AttributeServiceName)
	var newResource *Resource = nil

	if found {
		newResource = &Resource{
			Name: serviceName.Str(),
			Type: ResourceTypeService,
		}
	}

	if newResource == nil {
		s.Logger.Info("can't get resource name from resource")
		return Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	newResource.Name = strings.TrimSpace(newResource.Name)
	if newResource.Name == "" {
		s.Logger.Info("empty resource name")
		return Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	return *newResource
}

func (s *storage) inferFromResourceFromServerSpan(span ptrace.Span) Resource {
	var resource *Resource = nil

	userAgent, found := span.Attributes().Get(conventions.AttributeHTTPUserAgent)
	if found {
		resource = &Resource{
			Name: strings.Split(userAgent.Str(), "/")[0],
			Type: ResourceTypeBrowser,
		}
	}

	_, found = span.Attributes().Get(conventions.AttributeHTTPMethod)
	peerName, _ := span.Attributes().Get(conventions.AttributeNetPeerName)
	if found {
		resource = &Resource{
			Name: peerName.Str(),
			Type: ResourceTypeService,
		}
	}

	if resource == nil {
		s.Logger.Info(fmt.Sprintf("unknown span: %v", span.Attributes().AsRaw()))
		resource = &Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	resource.Name = strings.TrimSpace(resource.Name)
	if resource.Name == "" {
		s.Logger.Info(fmt.Sprintf("empty resource name inferred from server span, args: %v", span.Attributes().AsRaw()))
		return Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	s.Logger.Info(fmt.Sprintf("resource from server span, name: %v, attributes: %v", resource.Name, span.Attributes().AsRaw()))
	return *resource
}

func (s *storage) inferToResourceFromClientSpan(span ptrace.Span) Resource {
	var resource *Resource = nil

	dbSystem, foundDbSystem := span.Attributes().Get(conventions.AttributeDBSystem)
	dbName, foundDbName := span.Attributes().Get(conventions.AttributeDBName)
	if foundDbSystem {
		name := ""
		if foundDbName {
			name = fmt.Sprintf("%s.%s", dbSystem.Str(), dbName.Str())
		} else {
			name = dbSystem.Str()
		}
		resource = &Resource{
			Name: name,
			Type: ResourceTypeStorage,
		}
	}

	messagingSystem, foundMessagingSystem := span.Attributes().Get(conventions.AttributeMessagingSystem)
	destinationNameAnonymous, foundDestinationNameAnonymous := span.Attributes().Get("messaging.destination.anonymous")
	destinationName, foundDestinationName := span.Attributes().Get("messaging.destination.name")
	destinationTemplate, foundDestinationTemplate := span.Attributes().Get("messaging.destination.template")
	if foundMessagingSystem {
		name := messagingSystem.Str()

		if foundDestinationTemplate {
			name = destinationTemplate.Str()
		} else if foundDestinationNameAnonymous && !destinationNameAnonymous.Bool() && foundDestinationName {
			name = destinationName.Str()
		}

		resource = &Resource{
			Name: name,
			Type: ResourceTypeService,
		}
	}

	userAgent, found := span.Attributes().Get(conventions.AttributeHTTPUserAgent)
	if found {
		resource = &Resource{
			Name: strings.Split(userAgent.Str(), "/")[0],
			Type: ResourceTypeBrowser,
		}
	}

	_, found = span.Attributes().Get(conventions.AttributeHTTPMethod)
	peerName, _ := span.Attributes().Get(conventions.AttributeNetPeerName)
	if found {
		resource = &Resource{
			Name: peerName.Str(),
			Type: ResourceTypeService,
		}
	}

	rpcService, found := span.Attributes().Get(conventions.AttributeRPCService)
	if found {
		resource = &Resource{
			Name: rpcService.Str(),
			Type: ResourceTypeService,
		}
	}

	if resource == nil {
		s.Logger.Info(fmt.Sprintf("unknown span: %v", span.Attributes().AsRaw()))
		resource = &Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	resource.Name = strings.TrimSpace(resource.Name)
	if resource.Name == "" {
		s.Logger.Info(fmt.Sprintf("empty resource name inferred from client span, args: %v", span.Attributes().AsRaw()))
		return Resource{
			Name: "<nil-service-name>",
			Type: ResourceTypeUnknown,
		}
	}

	s.Logger.Info(fmt.Sprintf("resource from client span, name: %v, attributes: %v", resource.Name, span.Attributes().AsRaw()))
	return *resource
}

func (s *storage) newOperationFromSpanAndResource(spanAndResource SpanAndResource) Operation {
	name := strings.TrimSpace(spanAndResource.Span.Name())
	if name == "" {
		s.Logger.Info(fmt.Sprintf("empty operation name inferred from client span, args: %v", spanAndResource.Span.Attributes().AsRaw()))
		name = "<default>"
	}

	return Operation{
		Name:      name,
		DefinedIn: s.newResource(*spanAndResource.Resource),
	}
}

func (s *storage) inferToOperationFromClientSpan(span ptrace.Span) Operation {
	name := strings.TrimSpace(span.Name())
	if name == "" {
		s.Logger.Info(fmt.Sprintf("empty operation name inferred from client span, args: %v", span.Attributes().AsRaw()))
		name = "<default>"
	}

	return Operation{
		Name:      name,
		DefinedIn: s.inferToResourceFromClientSpan(span),
	}
}

// Shutdown will shutdown the exporter.
func (s *storage) Shutdown(ctx context.Context) error {
	return s.Writer.Close(ctx)
}
