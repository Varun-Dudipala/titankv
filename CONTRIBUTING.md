# Contributing to TitanKV

Thanks for your interest in contributing to TitanKV! This document provides guidelines for contributing to this distributed key-value store project.

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue with:
- Clear, descriptive title
- Steps to reproduce the bug
- Expected vs actual behavior
- Environment details (Java version, OS, cluster configuration)
- Logs or stack traces if applicable

### Suggesting Features

Feature requests are welcome! Please open an issue with:
- Clear description of the feature
- Use case explaining why it would be valuable
- Any implementation ideas you have
- Consider distributed systems trade-offs (CAP theorem)

### Pull Requests

1. **Fork the repository** and create a new branch from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following our code style guidelines

3. **Test your changes**
   - Run unit tests: `mvn test`
   - Test with a local cluster
   - Verify no compilation warnings
   - Test edge cases (network failures, node crashes)

4. **Commit your changes** with clear, descriptive messages
   ```bash
   git commit -m "Add: feature description"
   ```

5. **Push to your fork** and submit a pull request
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Describe your PR** with:
   - What changes you made
   - Why you made them
   - Any performance implications
   - Test results or benchmarks

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/titankv.git
cd titankv

# Build the project
mvn clean package

# Run tests
mvn test

# Start a local cluster
./scripts/start-cluster.sh
```

## Code Style Guidelines

### Java
- Follow standard Java naming conventions
- Use Java 17 features where appropriate
- Add Javadoc comments for public APIs
- Keep methods focused and concise
- Use meaningful variable names

### Code Organization
- Core storage logic in `core/`
- Cluster management in `cluster/`
- Network protocol in `network/`
- Consistency logic in `consistency/`
- Tests mirror main package structure

### Naming Conventions
- Classes: `PascalCase` (e.g., `ConsistentHashRing`)
- Methods: `camelCase` (e.g., `getNodeForKey()`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `MAGIC_NUMBER`)
- Packages: lowercase (e.g., `com.titankv.cluster`)

### Code Quality
- Write clean, readable code
- Add comments for complex algorithms (consistent hashing, gossip)
- Use appropriate data structures (ConcurrentHashMap, etc.)
- Handle errors and edge cases gracefully
- Remove debug print statements before committing

## Testing

### Unit Tests
```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=ConsistentHashTest

# Run with coverage
mvn test jacoco:report
```

### Integration Testing
Test your changes with a real cluster:
```bash
# Start 3-node cluster
./scripts/start-cluster.sh

# In another terminal, run benchmark
./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003
```

### Performance Testing
If your changes affect performance:
- Run benchmarks before and after
- Document performance characteristics
- Consider trade-offs (latency vs throughput vs consistency)

## Architecture Guidelines

### Distributed Systems Principles
- **CAP Theorem**: Consider consistency, availability, partition tolerance trade-offs
- **Eventual Consistency**: Design for asynchronous replication
- **Fault Tolerance**: Handle node failures gracefully
- **Idempotence**: Operations should be safe to retry

### Performance Considerations
- Use non-blocking I/O where possible
- Minimize object allocations in hot paths
- Profile before optimizing
- Consider network bandwidth and latency

### Protocol Changes
If modifying the binary protocol:
1. Document the change in README
2. Consider backward compatibility
3. Update protocol version if needed
4. Test with mixed-version clusters

## Adding New Features

### Storage Features
- Ensure thread-safety with ConcurrentHashMap
- Consider memory implications
- Add appropriate tests

### Cluster Features
- Test with various cluster sizes
- Handle network partitions
- Test failure scenarios

### Protocol Features
- Update command codes if needed
- Document new message formats
- Ensure efficient serialization

## Questions?

Feel free to open an issue with the `question` label, or reach out to [@varun-dudipala](https://github.com/varun-dudipala).

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on what's best for the project
- Show empathy towards other contributors
- Welcome newcomers to distributed systems

Thank you for contributing to TitanKV!
