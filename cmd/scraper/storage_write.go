package main

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"cloud.google.com/go/civil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	apiRequestBQWriter *bqStorageWriter
	ridershipBQWriter  *bqStorageWriter
)

type bqStorageWriter struct {
	ctx             context.Context
	client          *managedwriter.Client
	tableID         string
	stream          *managedwriter.ManagedStream
	descriptor      protoreflect.MessageDescriptor
	descriptorProto *descriptorpb.DescriptorProto
	mu              sync.Mutex
}

func newBQStorageWriter(ctx context.Context, client *managedwriter.Client, tableID string, schemaSource any) (*bqStorageWriter, error) {
	messageDescriptor, descriptorProto, err := bqStorageDescriptor(tableID, schemaSource)
	if err != nil {
		return nil, err
	}
	return &bqStorageWriter{
		ctx:             ctx,
		client:          client,
		tableID:         tableID,
		descriptor:      messageDescriptor,
		descriptorProto: descriptorProto,
	}, nil
}

func (w *bqStorageWriter) open() error {
	stream, err := w.client.NewManagedStream(w.ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(projectID, bqDatasetID, w.tableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(w.descriptorProto),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return fmt.Errorf("open %s write stream: %w", w.tableID, err)
	}
	w.stream = stream
	return nil
}

func bqStorageDescriptor(tableID string, schemaSource any) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	schema, err := bigquery.InferSchema(schemaSource)
	if err != nil {
		return nil, nil, fmt.Errorf("infer %s schema: %w", tableID, err)
	}
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("convert %s schema: %w", tableID, err)
	}
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(storageSchema, "root")
	if err != nil {
		return nil, nil, fmt.Errorf("build %s descriptor: %w", tableID, err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, fmt.Errorf("%s schema did not produce a message descriptor", tableID)
	}
	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize %s descriptor: %w", tableID, err)
	}
	return messageDescriptor, descriptorProto, nil
}

func (w *bqStorageWriter) Append(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	encoded := make([][]byte, 0, len(rows))
	for _, row := range rows {
		payload, err := encodeBQProtoRow(w.descriptor, row)
		if err != nil {
			return err
		}
		encoded = append(encoded, payload)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stream == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	result, err := w.stream.AppendRows(ctx, encoded)
	if err != nil {
		return err
	}
	_, err = result.GetResult(ctx)
	return err
}

func encodeBQProtoRow(descriptor protoreflect.MessageDescriptor, row map[string]any) ([]byte, error) {
	message := dynamicpb.NewMessage(descriptor)
	for name, raw := range row {
		if raw == nil {
			continue
		}
		field := descriptor.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			return nil, fmt.Errorf("unknown BigQuery field %q", name)
		}
		value, err := bqProtoValue(field, raw)
		if err != nil {
			return nil, fmt.Errorf("encode BigQuery field %q: %w", name, err)
		}
		message.Set(field, value)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal BigQuery row: %w", err)
	}
	return payload, nil
}

func (w *bqStorageWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stream == nil {
		return nil
	}
	return w.stream.Close()
}

func bqProtoValue(field protoreflect.FieldDescriptor, raw any) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		value, ok := raw.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("got %T, want string", raw)
		}
		return protoreflect.ValueOfString(value), nil
	case protoreflect.BoolKind:
		value, ok := raw.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("got %T, want bool", raw)
		}
		return protoreflect.ValueOfBool(value), nil
	case protoreflect.DoubleKind:
		value, ok := raw.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("got %T, want float64", raw)
		}
		return protoreflect.ValueOfFloat64(value), nil
	case protoreflect.Int64Kind:
		value, ok := raw.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("got %T, want int64", raw)
		}
		return protoreflect.ValueOfInt64(value), nil
	case protoreflect.Int32Kind:
		value, ok := raw.(int32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("got %T, want int32", raw)
		}
		return protoreflect.ValueOfInt32(value), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported proto kind %s", field.Kind())
	}
}

func bqDateValue(date civil.Date) int32 {
	return int32(date.DaysSince(civil.Date{Year: 1970, Month: 1, Day: 1}))
}
