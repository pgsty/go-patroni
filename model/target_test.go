package model_test

import (
	"testing"

	"github.com/pgsty/go-patroni/model"
)

func TestTargetNormalizeAndRoundTrip(t *testing.T) {
	group := 7
	target := model.Target{
		Context:   "prod/east",
		Namespace: "//service//patroni/",
		Scope:     "orders blue",
		Group:     &group,
		Member:    "db/02",
	}.Normalize()
	if target.Context != "prod/east" || target.Namespace != "/service/patroni" || target.Scope != "orders blue" || target.Member != "db/02" {
		t.Fatalf("unexpected normalized target: %#v", target)
	}
	if err := target.Validate(true); err != nil {
		t.Fatalf("valid target rejected: %v", err)
	}
	clusterID := target.ClusterID()
	memberID := target.MemberID()
	if clusterID == memberID || clusterID == "" || memberID == "" {
		t.Fatalf("invalid canonical IDs cluster=%q member=%q", clusterID, memberID)
	}
	cluster, err := model.ParseTargetID(clusterID)
	if err != nil {
		t.Fatalf("parse cluster ID: %v", err)
	}
	if cluster.Member != "" || cluster.Context != target.Context || cluster.Namespace != target.Namespace || cluster.Scope != target.Scope || cluster.Group == nil || *cluster.Group != group {
		t.Fatalf("cluster round trip mismatch: want %#v got %#v", target, cluster)
	}
	member, err := model.ParseTargetID(memberID)
	if err != nil {
		t.Fatalf("parse member ID: %v", err)
	}
	if member.Member != target.Member {
		t.Fatalf("member round trip mismatch: want %#v got %#v", target, member)
	}
}

func TestTargetDefaultsAndValidation(t *testing.T) {
	target := (model.Target{Scope: "alpha"}).Normalize()
	if target.Context != "default" || target.Namespace != "/service" {
		t.Fatalf("defaults mismatch: %#v", target)
	}
	if err := target.Validate(true); err != nil {
		t.Fatal(err)
	}
	if err := (model.Target{}).Normalize().Validate(false); err != nil {
		t.Fatalf("discovery target should not require scope: %v", err)
	}
	if err := (model.Target{}).Normalize().Validate(true); err == nil {
		t.Fatal("implicit cluster target must require a scope")
	}
	negative := -1
	if err := (model.Target{Scope: "alpha", Group: &negative}).Normalize().Validate(true); err == nil {
		t.Fatal("negative Citus group accepted")
	}
}

func TestTargetRejectsMalformedIDs(t *testing.T) {
	for _, value := range []string{"", "boar:cluster", "other:cluster/default/x/y/-", "boar:member/default/x/y/-", "boar:cluster/default/x/y/not-an-int"} {
		if _, err := model.ParseTargetID(value); err == nil {
			t.Errorf("malformed target ID accepted: %q", value)
		}
	}
}

func TestTargetParsesLegacyBOARIDs(t *testing.T) {
	legacy, err := model.ParseTargetID("boar:member/default/%2Fservice/alpha/-/node-a")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Context != "default" || legacy.Namespace != "/service" || legacy.Scope != "alpha" || legacy.Member != "node-a" {
		t.Fatalf("legacy target mismatch: %#v", legacy)
	}
	if got := legacy.MemberID(); got != "patroni:member/default/%2Fservice/alpha/-/node-a" {
		t.Fatalf("legacy ID did not canonicalize to public prefix: %q", got)
	}
}

func FuzzTargetIDRoundTrip(f *testing.F) {
	f.Add("default", "/service", "alpha", "node-1")
	f.Add("prod/east", "/nested/ns", "scope with spaces", "member/slash")
	f.Fuzz(func(t *testing.T, contextName, namespace, scope, member string) {
		if contextName == "" || scope == "" || member == "" {
			return
		}
		target := (model.Target{Context: contextName, Namespace: namespace, Scope: scope, Member: member}).Normalize()
		if target.Member == "" {
			return
		}
		if err := target.Validate(true); err != nil {
			return
		}
		parsed, err := model.ParseTargetID(target.MemberID())
		if err != nil {
			t.Fatalf("parse canonical ID: %v", err)
		}
		if parsed.Context != target.Context || parsed.Namespace != target.Namespace || parsed.Scope != target.Scope || parsed.Member != target.Member {
			t.Fatalf("round trip mismatch: %#v != %#v", parsed, target)
		}
	})
}
