# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Eligible key-affinity requests use every rack in the selected datacenter while
  ordinary requests retain rack-aware routing. The derived affinity scope keeps
  explicit fallback order, deduplicates equivalent widened scopes by their first
  occurrence, and reuses matching ordinary discovery results.

## [1.0.0] - 2026-10-02

### Added

- Initial release of the Rust client wrapper for ScyllaDB Alternator.
- Client-side discovery, routing scopes, and load balancing for Alternator nodes.
- Request and response compression, custom HTTP clients, and native TLS roots.
- DynamoDB client, configuration, and builder API compatibility checks.

[Unreleased]: https://github.com/scylladb/alternator-client-rust/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/scylladb/alternator-client-rust/releases/tag/v1.0.0
