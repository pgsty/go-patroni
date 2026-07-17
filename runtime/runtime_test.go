package runtime

import (
	"context"
	"testing"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/model"
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

func TestEnvironmentCopiesEmbeddingVersionRange(t *testing.T) {
	document, err := config.Parse([]byte("scope: demo\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	patroni4, err := model.NewVersionRange("4.0.0", "5.0.0")
	if err != nil {
		t.Fatal(err)
	}
	environment, err := NewEnvironment(context.Background(), EnvironmentOptions{
		Document: document, SupportedPatroniRange: &patroni4,
	})
	if err != nil {
		t.Fatal(err)
	}
	patroni4.Min = model.Version{Major: 3}
	if environment.supportedRange == nil || environment.supportedRange.Min.Major != 4 {
		t.Fatalf("environment version policy aliases caller memory: %#v", environment.supportedRange)
	}
}
