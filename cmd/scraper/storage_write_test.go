package main

import (
	"testing"

	"cloud.google.com/go/civil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestEncodeBQProtoRow(t *testing.T) {
	descriptor, _, err := bqStorageDescriptor(apiRequestBQTable, apiRequestObservation{})
	if err != nil {
		t.Fatal(err)
	}
	row := map[string]any{
		"service_date": bqDateValue(civil.Date{Year: 2026, Month: 9, Day: 1}),
		"observed_at":  int64(1_788_282_000_000_000),
		"source":       apiSourceRidership,
		"endpoint":     "api.actransit.org/transit/actrealtime/actrealtimeattribute",
		"latency_ms":   123.5,
		"success":      true,
		"outcome":      "success",
		"ingested_at":  int64(1_788_282_001_000_000),
	}
	payload, err := encodeBQProtoRow(descriptor, row)
	if err != nil {
		t.Fatal(err)
	}
	message := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(payload, message); err != nil {
		t.Fatal(err)
	}

	field := func(name string) protoreflect.FieldDescriptor {
		return descriptor.Fields().ByName(protoreflect.Name(name))
	}
	if got := message.Get(field("source")).String(); got != apiSourceRidership {
		t.Fatalf("source = %q", got)
	}
	if got := message.Get(field("latency_ms")).Float(); got != 123.5 {
		t.Fatalf("latency_ms = %v", got)
	}
	if got := message.Get(field("service_date")).Int(); got != int64(bqDateValue(civil.Date{Year: 2026, Month: 9, Day: 1})) {
		t.Fatalf("service_date = %d", got)
	}
	if message.Has(field("status_code")) {
		t.Fatal("nullable status_code unexpectedly present")
	}
}

func TestEncodeBQProtoRowRejectsWrongType(t *testing.T) {
	descriptor, _, err := bqStorageDescriptor(apiRequestBQTable, apiRequestObservation{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeBQProtoRow(descriptor, map[string]any{"latency_ms": int64(123)}); err == nil {
		t.Fatal("expected type error")
	}
}
