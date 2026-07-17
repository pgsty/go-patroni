// Package postgres implements BOAR's native one-shot PostgreSQL query SDK.
//
// It uses one pgx connection per query and contains no DCS lookup, member/role
// selection, CLI rendering, pool, SQL logger, or Server endpoint. Query data is
// streamed through a caller-owned Sink; the collecting helper applies explicit
// row and byte limits and reports truncation without abandoning the protocol
// stream.
package postgres
