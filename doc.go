// Package patroni provides a native Go client for the Patroni REST API.
//
// The root package owns HTTP wire contracts and transport behavior. Higher
// level packages in this module add Patroni configuration, DCS state,
// PostgreSQL queries, patronictl-compatible orchestration, and CLI adapters.
package patroni
