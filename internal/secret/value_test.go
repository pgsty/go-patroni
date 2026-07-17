package secret_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/pgsty/go-patroni/internal/secret"
)

func TestValueNeverFormatsOrMarshalsPlaintext(t *testing.T) {
	const plaintext = "__BOAR_FAKE_CREDENTIAL_7c86__"
	value := secret.New(plaintext)
	if !value.IsSet() || value.Reveal() != plaintext {
		t.Fatal("secret explicit access contract is broken")
	}
	outputs := []string{
		value.String(),
		value.GoString(),
		fmt.Sprint(value),
		fmt.Sprintf("%#v", value),
		value.LogValue().String(),
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	outputs = append(outputs, string(encoded))
	for _, output := range outputs {
		if strings.Contains(output, plaintext) {
			t.Fatalf("secret leaked from formatter/marshaler: %q", output)
		}
		if !strings.Contains(output, secret.Redacted) {
			t.Fatalf("set secret did not render the redaction marker: %q", output)
		}
	}
	empty := secret.New("")
	if empty.IsSet() || empty.String() != "" {
		t.Fatalf("empty secret should remain distinguishable: %q", empty.String())
	}
	if value.LogValue().Kind() != slog.KindString {
		t.Fatalf("secret slog value has unsafe kind %s", value.LogValue().Kind())
	}
}

func TestRedactKnownSecretsAndCredentialShapes(t *testing.T) {
	known := secret.New("known-value-46b0")
	input := strings.Join([]string{
		"known=known-value-46b0",
		"Authorization: Bearer bearer-value-19f0",
		"Proxy-Authorization: Basic proxy-basic-value-a312",
		"Authorization: Basic basic-value-19f0",
		"Cookie: boar_session=cookie-value-8821; csrf=cookie-token-71a2",
		"Set-Cookie: boar_session=set-cookie-value-22f1; HttpOnly",
		"postgres://user:uri-password-98a1@db.example/app",
		"host=db.example user=alice password=keyword-password-4421 dbname=app",
		"https://api.example.invalid/path?access_token=query-token-42f3&safe=yes",
		"api_key=key-value-api-93f1 safe=yes",
		"token: yaml-token-f381",
		"password: yaml-password-a7f2",
		`{"client_secret":"json-client-secret-b2c4","safe":"visible"}`,
		"private_key: yaml-private-key-c8a1",
	}, "\n")
	redacted := secret.Redact(input, known)
	for _, forbidden := range []string{
		"known-value-46b0", "bearer-value-19f0", "proxy-basic-value-a312", "basic-value-19f0",
		"cookie-value-8821", "cookie-token-71a2", "set-cookie-value-22f1", "uri-password-98a1",
		"keyword-password-4421", "query-token-42f3", "key-value-api-93f1", "yaml-token-f381",
		"yaml-password-a7f2", "json-client-secret-b2c4", "yaml-private-key-c8a1",
	} {
		if strings.Contains(redacted, forbidden) {
			t.Errorf("credential shape leaked %q in %q", forbidden, redacted)
		}
	}
	if strings.Count(redacted, secret.Redacted) < 14 {
		t.Fatalf("expected every credential shape to be visibly redacted: %q", redacted)
	}
}

func FuzzRedactKnownValue(f *testing.F) {
	f.Add("prefix", "secret-value", "suffix")
	f.Add("postgres://u:", "p@ss word", "@host/db")
	f.Fuzz(func(t *testing.T, prefix, raw, suffix string) {
		// Prefixing a hex encoding makes the searched token disjoint from the
		// fixed redaction marker. Otherwise a tiny value such as "]0" can be
		// recreated accidentally across the marker/suffix boundary even though
		// the original occurrence was replaced correctly.
		plaintext := "known-" + hex.EncodeToString([]byte(raw))
		output := secret.Redact(prefix+plaintext+suffix, secret.New(plaintext))
		if strings.Contains(output, plaintext) {
			t.Fatalf("known value was not redacted")
		}
	})
}
