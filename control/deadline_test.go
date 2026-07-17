package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

func TestPatroniWriteDeadlineRetainsUnknownOutcomeEvidence(t *testing.T) {
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		received <- struct{}{}
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	client, err := patroni.NewClient(patroni.ClientOptions{Timeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := readFixtureSnapshot()
	entries := append([]dcs.Entry(nil), snapshot.Entries...)
	for index := range entries {
		entries[index].Value = append([]byte(nil), entries[index].Value...)
		if entries[index].RelativePath == "members/node-a" {
			entries[index].Value = []byte(strings.ReplaceAll(string(entries[index].Value), "https://node-a:8008", server.URL))
		}
	}
	snapshot = dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision, entries)
	service, err := NewService(ServiceOptions{
		Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": snapshot}},
		Patroni:   client,
		Clock:     func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			return "deadline-restart-operation"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := RestartRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny,
	}
	prepared := service.PrepareRestart(context.Background(), request)
	if prepared.Outcome != Succeeded {
		t.Fatalf("prepare deadline restart: %#v", prepared)
	}
	caller, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := service.ExecuteRestart(caller, request, prepared.Data)
	select {
	case <-received:
	default:
		t.Fatal("Patroni test server did not receive the timed-out write")
	}
	if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown ||
		len(result.Data.Members) != 1 || result.Data.Members[0].SendState != SendMaybeSent ||
		result.Data.Members[0].Verification != Unverified || result.Data.Members[0].Error == nil ||
		!errors.Is(result.Data.Members[0].Error, context.DeadlineExceeded) {
		t.Fatalf("timed-out Patroni write lost UNKNOWN evidence: %#v", result)
	}
	if len(result.Data.Members[0].Evidence) == 0 || result.Data.Members[0].Evidence[0].Source != EvidencePatroni ||
		result.Data.Members[0].Evidence[0].Path != "/restart" || result.Data.Members[0].Evidence[0].SendState != SendMaybeSent {
		t.Fatalf("timed-out Patroni write evidence mismatch: %#v", result.Data.Members[0].Evidence)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("timed-out Patroni write result invalid: %v", err)
	}
}
