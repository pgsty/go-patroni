package cli

import (
	"reflect"
	"testing"
)

func TestMachineSuccessCatalogIsExhaustiveAndRuntimeChecked(t *testing.T) {
	catalog := MachineSchemaCatalog()
	if len(catalog) != 22 {
		t.Fatalf("machine success catalog count=%d want=22", len(catalog))
	}
	metadata := machineMetadata{RequestID: "catalog-test", ObservedAt: "2026-07-13T12:34:56Z", Warnings: []string{}}
	for kind, dataType := range catalog {
		if kind == "" || dataType == nil || dataType.Kind() == reflect.Pointer {
			t.Errorf("invalid catalog entry %q -> %v", kind, dataType)
			continue
		}
		data := reflect.New(dataType).Elem().Interface()
		if _, err := newMachineSuccessEnvelope(kind, metadata, data); err != nil {
			t.Errorf("registered kind %s rejected its exact DTO %s: %v", kind, dataType, err)
		}
	}
	if _, err := newMachineSuccessEnvelope("UnregisteredResult", metadata, struct{}{}); err == nil {
		t.Fatal("unregistered machine success kind was accepted")
	}
	if _, err := newMachineSuccessEnvelope(machineKindDSN, metadata, struct{}{}); err == nil {
		t.Fatal("registered machine success kind accepted the wrong DTO type")
	}
}
