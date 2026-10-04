# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Replaced `/localnodes` discovery with DynamoDB scans of ScyllaDB's
  `system.local` and `system.peers` tables, enabling one seed host to discover
  the full multi-datacenter topology.
- Discovery now requires ScyllaDB 4.1 or newer and, when authorization is
  enforced, `SELECT` permission on both system tables. System-table membership
  is not a liveness signal, so unavailable members remain eligible for routing
  until the cluster topology removes them and do not activate scope fallbacks.
- ScyllaDB 5.2 or newer is recommended when bind and client-advertised
  addresses differ; earlier versions can expose an unreachable bind address in
  `system.local.rpc_address`.

## [1.0.0] - 2026-10-02

### Added

- Initial release of the Rust client wrapper for ScyllaDB Alternator.
- Client-side discovery, routing scopes, and load balancing for Alternator nodes.
- Request and response compression, custom HTTP clients, and native TLS roots.
- DynamoDB client, configuration, and builder API compatibility checks.

[Unreleased]: https://github.com/scylladb/alternator-client-rust/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/scylladb/alternator-client-rust/releases/tag/v1.0.0
