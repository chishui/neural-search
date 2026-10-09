# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
- [Sparse ANN] Add a Lucene-side reader and scorer for a `forward_index: per_block` native engine file (nsparse `disk_seismic_sq`), parsing it through an `IndexInput` and prefetching the blocks a query selects rather than having nsparse mmap the file. Used automatically when no filesystem path can be resolved from the store, which the native path cannot serve at all, or on demand via `plugins.neural_search.sparse.native_engine_index_input_search_enabled`

### Bug Fixes

### Infrastructure

### Documentation

### Maintenance

### Refactoring

