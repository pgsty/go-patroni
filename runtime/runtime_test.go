package runtime

import (
	"context"
	"testing"

	"github.com/pgsty/go-patroni/config"
)

func TestEnvironmentEmbeddingIdentity(t *testing.T) {
	document, err := config.Parse([]byte("scope: demo\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}

	embedded, err := NewEnvironment(context.Background(), EnvironmentOptions{
		Document: document, UserAgent: "pig/2.0", ProductVersion: "pig 2.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if embedded.userAgent != "pig/2.0" || embedded.productVersion != "pig 2.0" {
		t.Fatalf("embedding identity = %q, %q", embedded.userAgent, embedded.productVersion)
	}

	defaults, err := NewEnvironment(context.Background(), EnvironmentOptions{Document: document})
	if err != nil {
		t.Fatal(err)
	}
	if defaults.userAgent == "" || defaults.productVersion == "" {
		t.Fatalf("default identity = %q, %q", defaults.userAgent, defaults.productVersion)
	}
}
