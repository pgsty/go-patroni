//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	patronipostgres "github.com/pgsty/go-patroni/postgres"
)

func TestPostgreSQLNativeQueryTLSAuthMultiResultLimitsErrorAndCancel(t *testing.T) {
	if os.Getenv("GO_PATRONI_TEST_POSTGRES_ISOLATED") != "1" {
		t.Fatal("refusing PostgreSQL integration test without GO_PATRONI_TEST_POSTGRES_ISOLATED=1")
	}
	host := os.Getenv("GO_PATRONI_TEST_POSTGRES_HOST")
	if host != "127.0.0.1" {
		t.Fatalf("refusing PostgreSQL integration test against non-loopback host %q", host)
	}
	portNumber, err := strconv.ParseUint(os.Getenv("GO_PATRONI_TEST_POSTGRES_PORT"), 10, 16)
	if err != nil || portNumber == 0 {
		t.Fatal("GO_PATRONI_TEST_POSTGRES_PORT must be a valid isolated loopback port")
	}
	caPEM, err := os.ReadFile(os.Getenv("GO_PATRONI_TEST_POSTGRES_CA"))
	if err != nil {
		t.Fatal("read isolated PostgreSQL CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("parse isolated PostgreSQL CA")
	}

	client, err := patronipostgres.NewClient(patronipostgres.ClientOptions{
		Timeout:       10 * time.Second,
		DefaultLimits: patronipostgres.Limits{MaxRows: 1_000, MaxBytes: 4 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	options := patronipostgres.NewConnectionOptions("")
	options.Host = host
	options.Port = uint16(portNumber)
	options.Database = "postgres"
	options.Username = "postgres"
	options.ApplicationName = "go-patroni-m2-integration"
	options.ConnectTimeout = 5 * time.Second

	// The generated lab CA is intentionally not in the system pool. A default
	// verify-full connection must reject it before the trusted CA is supplied.
	_, err = client.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: "select 1"})
	var typed *patronipostgres.Error
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorConnect {
		t.Fatalf("verify-full did not reject an untrusted server certificate: %#v", err)
	}
	options = options.WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
	hostnameMismatch := options
	hostnameMismatch.Host = "localhost"
	_, err = client.Query(context.Background(), hostnameMismatch, patronipostgres.QueryRequest{SQL: "select 1"})
	var hostnameError x509.HostnameError
	if !errors.As(err, &hostnameError) {
		t.Fatalf("verify-full did not reject a PostgreSQL certificate hostname mismatch: %#v", err)
	}

	missingCredentials := options
	missingCredentials.Username = "go_patroni_missing_credentials"
	_, err = client.Query(context.Background(), missingCredentials, patronipostgres.QueryRequest{SQL: "select 1"})
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorDatabase || typed.SQLState != "28P01" {
		t.Fatalf("real PostgreSQL accepted a connection without a matching credential source: %#v", err)
	}

	primary, err := client.QueryChecked(context.Background(), options, patronipostgres.RecoveryPrimary,
		patronipostgres.QueryRequest{SQL: "select pg_catalog.pg_is_in_recovery() as in_recovery"})
	if err != nil || len(primary.Sets) != 1 || len(primary.Sets[0].Rows) != 1 || primary.Sets[0].Rows[0][0].Text != "f" {
		t.Fatalf("real same-connection primary role check failed: result=%#v err=%v", primary, err)
	}
	_, err = client.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: `
drop table if exists go_patroni_m3_role_guard;
create table go_patroni_m3_role_guard(value integer);
`})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.QueryChecked(context.Background(), options, patronipostgres.RecoveryStandby,
		patronipostgres.QueryRequest{SQL: "insert into go_patroni_m3_role_guard values (1)"})
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorRoleMismatch {
		t.Fatalf("real primary was accepted as a standby: %#v", err)
	}
	guard, err := client.Query(context.Background(), options,
		patronipostgres.QueryRequest{SQL: "select count(*) from go_patroni_m3_role_guard"})
	if err != nil || len(guard.Sets) != 1 || len(guard.Sets[0].Rows) != 1 || guard.Sets[0].Rows[0][0].Text != "0" {
		t.Fatalf("role-mismatched user SQL reached PostgreSQL: result=%#v err=%v", guard, err)
	}

	const multiSQL = `
create temporary table go_patroni_query_fixture(id integer primary key, payload text, note text);
insert into go_patroni_query_fixture values (1, 'alpha', null), (2, 'beta', 'present');
select id, payload, note, current_setting('application_name') as app,
       (select ssl from pg_catalog.pg_stat_ssl where pid = pg_catalog.pg_backend_pid()) as tls
  from go_patroni_query_fixture order by id;
update go_patroni_query_fixture set payload = upper(payload) where id = 2;
`
	result, err := client.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: multiSQL})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sets) != 4 {
		t.Fatalf("got %d PostgreSQL result sets, want 4: %#v", len(result.Sets), result)
	}
	if result.Sets[0].CommandTag != "CREATE TABLE" || result.Sets[1].CommandTag != "INSERT 0 2" ||
		result.Sets[1].RowsAffected != 2 || result.Sets[3].CommandTag != "UPDATE 1" || result.Sets[3].RowsAffected != 1 {
		t.Fatalf("PostgreSQL command tags mismatch: %#v", result)
	}
	selected := result.Sets[2]
	if len(selected.Columns) != 5 || len(selected.Rows) != 2 || selected.Columns[0].Name != "id" ||
		selected.Rows[0][0].Text != "1" || selected.Rows[0][2].Null != true ||
		selected.Rows[1][2].Text != "present" || selected.Rows[0][3].Text != "go-patroni-m2-integration" ||
		selected.Rows[0][4].Text != "t" {
		t.Fatalf("PostgreSQL row/null/metadata/TLS projection mismatch: %#v", selected)
	}

	limited, err := client.Query(context.Background(), options, patronipostgres.QueryRequest{
		SQL:    "select value from (values (repeat('x', 64)), ('y')) as input(value)",
		Limits: patronipostgres.Limits{MaxRows: 10, MaxBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.Sets) != 1 || len(limited.Sets[0].Rows) != 0 || limited.Summary.ObservedRows != 2 ||
		limited.Summary.EmittedRows != 0 || !limited.Summary.Truncated || limited.Sets[0].CommandTag != "SELECT 2" {
		t.Fatalf("real PostgreSQL prefix truncation/drain mismatch: %#v", limited)
	}

	const missingRelation = "go_patroni_m2_missing_relation"
	_, err = client.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: "select * from " + missingRelation})
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorDatabase || typed.SQLState != "42P01" ||
		strings.Contains(err.Error(), missingRelation) || strings.Contains(fmt.Sprintf("%#v", err), missingRelation) {
		t.Fatalf("real PostgreSQL SQLSTATE/redaction mismatch: %#v", err)
	}

	cancelContext, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	started := time.Now()
	_, err = client.Query(cancelContext, options, patronipostgres.QueryRequest{SQL: "select pg_catalog.pg_sleep(30)"})
	timer.Stop()
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("real PostgreSQL cancellation classification mismatch: %#v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("real PostgreSQL cancellation took %s", elapsed)
	}

	deadlineClient, err := patronipostgres.NewClient(patronipostgres.ClientOptions{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	_, err = deadlineClient.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: "select pg_catalog.pg_sleep(30)"})
	if !errors.As(err, &typed) || typed.Kind != patronipostgres.ErrorDeadline || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("real PostgreSQL default deadline classification mismatch: %#v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("real PostgreSQL default deadline took %s", elapsed)
	}

	afterCancel, err := client.Query(context.Background(), options, patronipostgres.QueryRequest{SQL: "select 1 as healthy"})
	if err != nil || len(afterCancel.Sets) != 1 || len(afterCancel.Sets[0].Rows) != 1 ||
		afterCancel.Sets[0].Rows[0][0].Text != "1" {
		t.Fatalf("query after canceled one-shot connection failed: result=%#v err=%v", afterCancel, err)
	}
}
