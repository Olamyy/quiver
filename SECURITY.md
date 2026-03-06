# Security Advisory Analysis

This document analyzes security vulnerabilities found in Quiver's dependencies and documents our risk assessment and mitigation strategies.

## Current Security Status

### RUSTSEC-2023-0071: RSA Marvin Attack (ACCEPTED RISK)

**Status**: Acknowledged and accepted  
**Severity**: Medium (5.9 CVSS score)  
**Advisory**: https://rustsec.org/advisories/RUSTSEC-2023-0071  

#### Vulnerability Details
- **Affected Crate**: `rsa 0.9.10`
- **Issue**: Timing side-channel attack against RSA PKCS#1 v1.5 decryption
- **Attack Vector**: Marvin attack for potential private key recovery
- **Dependency Path**: `rsa` ← `sqlx-mysql` ← `sqlx` ← `quiver-core`

#### Risk Assessment: **LOW RISK**

**Rationale for Acceptance**:

1. **No MySQL Usage**: Quiver only uses PostgreSQL adapters, not MySQL
   - The vulnerable RSA code path is in `sqlx-mysql` for MySQL SSL handshakes
   - Quiver explicitly uses PostgreSQL with modern TLS (not RSA PKCS#1 v1.5)

2. **PostgreSQL Uses Modern TLS**: 
   - PostgreSQL connections use `sslmode=require` with standard TLS 1.2/1.3
   - Modern TLS doesn't use RSA PKCS#1 v1.5 for key exchange
   - Uses ECDHE or DHE for forward secrecy

3. **Attack Requirements**:
   - Requires ability to observe timing of RSA decryption operations
   - Needs thousands of specially crafted SSL handshake attempts
   - Only affects MySQL SSL connections (not used by Quiver)

4. **Infrastructure Context**:
   - Quiver is a feature serving layer, not a cryptographic service
   - SSL/TLS is handled by PostgreSQL server, not by Quiver directly
   - Attack would target database server, not Quiver application

#### Verification Commands
```bash
# Audit ignoring this specific advisory
cargo audit --ignore RUSTSEC-2023-0071

# Verify PostgreSQL usage only (no MySQL)
grep -r "mysql" quiver-core/src/ || echo "No MySQL usage found"

# Check TLS configuration in examples
grep -r "sslmode=require" examples/
```

#### Monitoring Plan
- **Quarterly Review**: Check for updated sqlx versions that remove MySQL dependency
- **Security Scanning**: Continue monitoring for new vulnerabilities
- **Upgrade Path**: When sqlx provides PostgreSQL-only builds, migrate to those

#### Mitigation Measures
1. **Enforce TLS**: All PostgreSQL connections require `sslmode=require`
2. **No MySQL**: Quiver architecture explicitly avoids MySQL
3. **Network Security**: Deploy in secure networks with encrypted connections
4. **Regular Updates**: Keep sqlx and dependencies updated when fixes are available

---

## Security Best Practices

### Connection Security
- **PostgreSQL**: Always use `sslmode=require` or stronger
- **Redis**: Use `rediss://` (TLS) for production deployments
- **Credentials**: Store in environment variables, never in configuration files

### Input Validation
- All entity IDs and feature names are validated for injection attacks
- PostgreSQL identifiers are restricted to alphanumeric + underscore
- Redis keys are validated to prevent command injection

### Network Security
- gRPC should use TLS in production (`server.tls` configuration)
- Internal service-to-service communication should be encrypted
- Firewall rules should restrict database access to Quiver only

### Operational Security
- Regular security audits with `cargo audit`
- Dependency updates following security advisories
- Access logging enabled for compliance and incident response

---

## Security Reporting

If you discover security vulnerabilities:
1. **DO NOT** open public GitHub issues
2. Report privately to the maintainers
3. Include reproduction steps and impact assessment
4. Allow responsible disclosure timeline

---

## Compliance Notes

### SOC 2 / Enterprise Requirements
- All data connections encrypted in transit
- Access logging capabilities built-in
- Input validation prevents injection attacks
- Regular security dependency scanning

### GDPR / Data Privacy
- Quiver does not store personal data (passes through only)
- Feature data encryption depends on backend configuration
- Access logs can be configured to exclude sensitive data

---

*Last Updated: March 6, 2026*
*Next Review: June 6, 2026*