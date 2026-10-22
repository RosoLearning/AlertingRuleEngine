# Part 2: Executive Summary and Proposed Solution

## 1. Executive Summary

A Company needs a centralized, scalable, multi-tenant rule engine capable of processing telemetry from millions of IoT devices and triggering reliable business actions.

The solution should separate the platform into four primary capabilities:

1. **Telemetry ingestion and normalization:** validate incoming SenML data, map LwM2M resource paths to canonical measurements, and publish normalized events.
2. **Rule management and governance:** enable business users to create, validate, test, approve, activate, deactivate and roll back versioned rules.
3. **Complex event processing:** evaluate single-event conditions, rolling aggregates, temporal patterns and missing-data conditions using a stateful stream-processing engine.
4. **Alert and action execution:** reliably deliver notifications, webhooks, support tickets and remediation commands through independent workers and provider adapters.

### Recommended technology choices

| Capability | Recommended technology | Rationale |
|---|---|---|
| Event ingestion | Apache Kafka | Durable, partitioned, replayable event streaming |
| Rule management API | ASP.NET Core / .NET | Strong fit for APIs, governance and enterprise integration |
| Rule metadata store | PostgreSQL | Transactions, versioning, querying and audit metadata |
| Complex event processing | Apache Flink | Stateful event-time windows, checkpointing and recovery |
| Rule model | Typed declarative DSL / AST | Safe business-authored rules and predictable execution |
| Action execution | Kafka and independent .NET workers | Reliable asynchronous delivery and extensibility |
| Device metadata | Canonical replicated read model | Avoids synchronous dependence on the legacy monolith |
| Identity | Enterprise OIDC/OAuth 2.0 provider | Authentication and authorization |
| Deployment | Kubernetes | Portable container orchestration |
| Observability | OpenTelemetry, Prometheus and Grafana | Vendor-neutral telemetry and diagnostics |

Apache Flink is the recommended starting point for complex, stateful event processing. If actual requirements are limited to simple event filters, a lighter implementation may be justified. The final selection should follow representative workload benchmarks and operational-capability assessment.

### Core architecture principles

- Separate control-plane operations from the high-volume data plane.
- Normalize device telemetry into a stable, versioned event contract.
- Treat rules as immutable, versioned business assets.
- Use event-time processing and checkpointed state for temporal correctness.
- Assume duplicate delivery and design idempotent alert/action handling.
- Separate evaluation from external side effects.
- Enforce tenant authorization and resource quotas at every service boundary.
- Replicate legacy metadata into a platform-owned read model.
- Use portable open-source components where practical without rejecting valuable managed capabilities automatically.
- Build observability, security, recovery and auditability into the first production slice.

---

## 2. Target Architecture

### 2.1. Logical architecture

```text
                    CONTROL PLANE
                    -------------

 Business Users
      |
      v
 Rule Authoring UI
      |
      v
 Rule Management API <------> Tenant Configuration
      |
      v
 Rule Registry / Version Store
      |
      v
 Rule Validation / Simulation / Approval
      |
      v
 Rule Publication / Activation
      |
      v
 Rule Distribution
      |
      +----------------------------------+
                                         |
                                         v
                    DATA PLANE
                    ----------

 IoT Devices / Gateways
      |
      v
 SenML JSON Telemetry
      |
      v
 Apache Kafka: Raw Telemetry
      |
      v
 Telemetry Normalizer
      |
      +------> Quarantine / Dead-Letter Topic
      |
      v
 Kafka: Canonical Telemetry
      |
      v
 Stateful CEP Engine (Apache Flink)
      |
      +------> Keyed State / Event-Time Windows
      |
      v
 Alert Event / Action Command
      |
      v
 Kafka: Action Commands
      |
      v
 Action Dispatcher / Retry / Idempotency
      |
      +----------+-----------+-----------+-------------+
      |          |           |           |             |
      v          v           v           v             v
    Email       SMS        Webhook     Support      Remediation
                                        Ticket
```

Cross-cutting capabilities:

- Enterprise identity and authorization.
- Device identity and registration.
- Secrets management.
- Audit logging.
- Metrics, logs and distributed traces.
- Tenant quotas and usage attribution.
- Schema and contract governance.
- CI/CD, infrastructure as code and disaster recovery.

### 2.2. Component boundaries

| Boundary | Reason |
|---|---|
| Rule management vs. evaluation | Different scaling profiles and release cycles |
| Raw telemetry vs. normalized telemetry | Isolates protocol parsing from business logic |
| Rule evaluation vs. action execution | Prevents provider latency and outages from blocking processing |
| Shared engine vs. tenant configuration | Enables isolation and independent business policies |
| Legacy monolith vs. canonical metadata | Reduces coupling and supports eventual decommissioning |
| Platform infrastructure vs. business rule definitions | Allows business changes without platform redeployment |

These are logical boundaries. The first implementation can consolidate closely related services into fewer deployable units, provided the interfaces and ownership boundaries remain clear.

### 2.3. Stable integration contracts

Define and version the following contracts:

- Incoming SenML telemetry.
- Canonical normalized telemetry events.
- Rule definitions and immutable rule versions.
- Rule activation/deactivation events.
- Alert events.
- Action commands and delivery results.
- Device metadata and metadata-change events.
- Audit records and observability context.

All event contracts should have documented compatibility rules, schema validation and automated consumer-contract tests.

---

## 3. Telemetry Ingestion and Normalization

### 3.1. Ingestion flow

1. Consume the raw SenML JSON message from Kafka.
2. Parse the SenML record list and resolve base name and base time.
3. Validate device identity against trusted registration or gateway metadata.
4. Map LwM2M paths to canonical metric names and units.
5. Validate value types, units, timestamps and permitted ranges.
6. Generate or propagate a stable event identifier.
7. Add ingestion metadata and schema version.
8. Publish normalized measurements to a canonical Kafka topic.
9. Route malformed or unsupported records to a quarantine topic.
10. Record parsing, validation, duplicate and latency metrics.

### 3.2. Example normalized event

```json
{
  "schemaVersion": "1.0",
  "eventId": "stable-event-identifier",
  "tenantId": "division-supply-chain",
  "deviceId": "urn:dev:mac:036b2738c1a4",
  "metric": "temperature",
  "objectPath": "3303/0/5700",
  "value": 23.37,
  "unit": "Cel",
  "eventTime": "2025-04-30T...",
  "ingestedAt": "2026-10-22T...",
  "source": {
    "topic": "iot-telemetry",
    "partition": 12,
    "offset": 987654
  }
}
```

The timestamps and identifiers above are illustrative placeholders. Production code must derive them from the actual source event and Kafka metadata.

### 3.3. Duplicate and malformed messages

- Use stable source IDs when available.
- Avoid deduplication based solely on timestamps and values because legitimate measurements can have identical values.
- Quarantine invalid JSON, unsupported schemas and records missing required fields.
- Preserve enough source metadata to support diagnosis and replay.
- Handle valid measurements independently only when partial message acceptance is explicitly defined.
- Measure duplicates and late events so they do not silently distort rule evaluation.

### 3.4. Delivery semantics

| Stage | Target |
|---|---|
| Device to Kafka | Reliable delivery subject to device/gateway capability |
| Kafka consumption | At-least-once |
| Normalization | Idempotent processing where feasible |
| CEP state | Checkpointed, recoverable state |
| Alert publication | Durable, checkpoint-aware or transactional publication |
| Action delivery | At-least-once attempts with idempotency and reconciliation |

Exactly-once processing inside a stream processor does not automatically mean exactly-once effects in external systems. Email, SMS and webhook actions require separate idempotency and reconciliation strategies.

---

## 4. Rule Model and Lifecycle

### 4.1. Declarative rule model

Rules should be represented using a typed declarative DSL or AST rather than arbitrary code.

A rule should include:

- Rule ID and tenant ID.
- Immutable version number.
- Name and description.
- Target device/group scope.
- Metric and condition expression.
- Aggregation and time-window configuration.
- Trigger and recovery semantics.
- Severity and notification policy.
- Action references.
- Author, approver, timestamps and lifecycle status.

Example:

```json
{
  "ruleId": "low-voltage-001",
  "tenantId": "division-supply-chain",
  "version": 3,
  "name": "Low voltage sustained for five minutes",
  "scope": {
    "deviceGroup": "warehouse-sensors"
  },
  "condition": {
    "type": "aggregate",
    "metric": "voltage",
    "aggregation": "average",
    "window": {
      "type": "sliding",
      "duration": "PT5M",
      "allowedLateness": "PT30S"
    },
    "operator": "lt",
    "value": 10,
    "unit": "V"
  },
  "trigger": {
    "mode": "threshold-crossing",
    "cooldown": "PT10M"
  },
  "severity": "high",
  "actions": [
    {
      "actionType": "support-ticket",
      "configurationRef": "warehouse-ticket-policy"
    }
  ]
}
```

### 4.2. Rule safety and extensibility

Support a controlled set of operators:

- Numeric comparisons and logical operators.
- Minimum, maximum, average, sum, count and rate.
- Sliding and tumbling windows.
- Sustained conditions and threshold crossings.
- Sequence and composite event patterns.
- Missing-data and device-silence conditions.
- Approved metadata-based predicates.

Each operator must define input/output types, unit semantics, state requirements, complexity limits, missing-value behavior and supported engine versions.

### 4.3. Rule lifecycle

```text
Draft
  -> Validate
  -> Simulate / Replay
  -> Approve
  -> Publish Immutable Version
  -> Activate
  -> Monitor
  -> Disable / Roll Back / Retire
```

Rule updates must not mutate a version already used for evaluation. Every alert must identify the exact rule version that triggered it.

The platform must define what happens to temporal windows and existing alert state when a rule is modified or deactivated.

---

## 5. Multi-Tenancy

### 5.1. Recommended isolation model

Use logical tenant isolation by default, with stronger isolation for divisions requiring dedicated capacity or stronger security boundaries.

| Area | Isolation strategy |
|---|---|
| Rules | Tenant-scoped authorization and queries |
| Telemetry | Trusted tenant assignment and tenant-aware processing |
| Kafka | ACLs, quotas and separate topics where needed |
| CEP | Tenant-aware state, quotas and workload controls |
| Actions | Tenant-specific configuration references and permissions |
| Secrets | Least-privilege secret access; separate vaults where required |
| Monitoring | Tenant-safe dashboards and access policies |
| Compute | Shared pools by default; dedicated pools for high-risk or high-volume tenants |
| Cost | Attribute usage and dedicated capacity to divisions |

### 5.2. Prevent cross-tenant access

- Derive tenant identity from authenticated or trusted registration context.
- Enforce tenant authorization server-side.
- Validate that rules, device groups, actions and destinations belong to the same tenant.
- Apply least-privilege access to topics, stores and secrets.
- Audit administrative operations.
- Test cross-tenant access boundaries automatically.

### 5.3. Prevent noisy neighbors

Introduce per-tenant quotas for event rate, active rules, expensive operators, state size, action rate and resource usage.

Quotas should trigger explicit throttling, rejection, queuing or escalation policies—not silent loss of telemetry.

---

## 6. Stateful Evaluation and Correctness

### 6.1. Processing model

1. Consume normalized telemetry.
2. Assign event time and trusted device identity.
3. Partition work by the key required for the computation.
4. Evaluate stateless predicates.
5. Update keyed window state.
6. Evaluate aggregates and temporal patterns.
7. Detect rule transitions and suppression conditions.
8. Generate deterministic alert identifiers.
9. Publish alert/action commands durably.
10. Commit offsets and state according to the selected consistency model.

For device-local rules, partition by `tenantId + deviceId`. Cross-device rules may require repartitioning or a two-stage aggregation.

### 6.2. Window state

Use the stream processor's managed keyed state for:

- Rolling aggregates.
- Window buffers or incremental aggregates.
- Sequence progress.
- Threshold-crossing state.
- Cooldown timers.
- Bounded event deduplication.

Persist checkpoints to durable storage and recover by restoring state and replaying Kafka events.

Maintain durable alert history and action-delivery records separately when they are required for audit, reconciliation or support workflows.

### 6.3. Late, out-of-order and missing events

- Use event time for telemetry windows.
- Define watermarks and allowed lateness.
- Update window results for accepted late events when supported by the rule semantics.
- Route extremely late events to a correction or historical processing path.
- Track last-seen timestamps for device-silence rules.
- Distinguish device outages from ingestion or regional outages.
- Document whether averages are sample-weighted or time-weighted.

### 6.4. Example five-minute average

For sample-weighted readings:

\[
\text{Average} = \frac{\sum_{i=1}^{N} v_i}{N}
\]

Maintain rolling sum and count incrementally, removing samples that leave the window. Ensure duplicate measurements do not distort the result.

For irregularly spaced measurements, consider a time-weighted average if it better represents the physical quantity.

---

## 7. Alert Actions and Noise Reduction

### 7.1. Action architecture

```text
CEP Engine
    |
    v
Alert Event / Action Command
    |
    v
Durable Kafka Topic
    |
    v
Action Dispatcher
    |
    +--> Email Adapter
    +--> SMS Adapter
    +--> Webhook Adapter
    +--> Ticketing Adapter
    +--> PagerDuty / Slack Adapter
    +--> Controlled Remediation Adapter
```

Each adapter should implement configuration validation, execution, status reporting, failure classification and reconciliation.

### 7.2. Reliability mechanisms

- Durable action commands.
- Exponential backoff with jitter.
- Provider-specific retry policies.
- Circuit breakers and concurrency limits.
- Idempotency keys.
- Action-delivery ledger.
- Dead-letter queues.
- Command deadlines and expiry policies.
- Manual replay and reconciliation tools.

A timeout can mean that a provider executed the action but failed to return a response. Reconciliation and provider-supported idempotency are therefore essential.

### 7.3. Alert noise reduction

Support:

- Threshold crossing instead of repeated level-triggered alerts.
- Hysteresis and recovery thresholds.
- Sustained-condition requirements.
- Cooldown periods.
- Grouping and deduplication.
- Maintenance windows.
- Escalation policies.
- Per-tenant rate limits.
- Alert acknowledgement and resolution.

Maintain alert lifecycle state and suppression reasons. Suppression must not silently hide critical incidents or conceal a failing rule engine.

---

## 8. Quality Attributes, SLOs and Scale

### 8.1. Key quality attributes

- Scalability through partitioned Kafka topics and parallel stream processing.
- Correctness through versioned rules, event-time semantics and duplicate handling.
- Availability through redundant infrastructure and recoverable state.
- Extensibility through versioned contracts, operator plugins and action adapters.
- Security through strong identity, tenant isolation and least privilege.
- Operability through metrics, traces, logs, replay and recovery procedures.
- Cost efficiency through resource quotas, state-retention controls and usage attribution.
- Portability through open-source components and stable interfaces.

### 8.2. Illustrative SLOs

These targets must be validated using representative load and failure testing.

| SLO | Initial target |
|---|---|
| Simple-rule evaluation | 99% within 2 seconds of ingestion |
| Window-rule evaluation | 99% within 10 seconds after the configured evaluation boundary, excluding allowed-lateness delay |
| Action dispatch | 99% within 30 seconds under normal provider conditions |
| Rule activation | 99% within 60 seconds of approval |
| Management API availability | 99.9% monthly |
| Alert command durability | No silent loss under the defined failure model |
| Recovery | Meet explicitly agreed processing-recovery and state-restore targets |

Measure engine decision latency separately from external notification delivery latency.

### 8.3. Scaling to 10× volume

Estimate capacity using:

```text
Event rate = Device count × Measurements per device per second

Evaluation work ≈ Event rate × Applicable rules per event × Average rule cost
```

Potential bottlenecks include Kafka partitions, hot keys, expensive rule fan-out, state growth, checkpoint pressure, metadata enrichment, action-provider throttling and high-cardinality monitoring.

Mitigate them through representative benchmarks, incremental aggregation, selective rule matching, per-tenant quotas, independent action scaling and fault-injection tests.

---

## 9. Security, Observability and Operations

### 9.1. Security

- Authenticate devices or trusted gateways.
- Validate device identity against registration metadata.
- Integrate rule administration with enterprise identity.
- Enforce role-based access and separation of duties.
- Use short-lived workload credentials where supported.
- Encrypt data in transit and at rest.
- Protect secrets and rotate provider credentials.
- Validate webhook destinations to prevent SSRF.
- Restrict remediation to approved, least-privilege workflows.
- Audit rule creation, modification, approval, activation and rollback.
- Apply retention policies to telemetry, alerts and raw payloads.

### 9.2. Observability

Monitor four areas.

**Ingestion:** event rate, invalid payloads, duplicate rate, Kafka lag, late events and quarantine volume.

**Evaluation:** throughput, evaluation duration, rules per event, state size, checkpoint failures, restarts, activation lag and trigger counts.

**Actions:** command queue depth, oldest-command age, delivery latency, retries, provider throttling and dead-letter volume.

**Business outcomes:** alerts per device/rule/division, suppression rates, open incidents, resolution time and end-to-end latency.

### 9.3. Detect silent failures

- Inject synthetic telemetry and verify expected alerts end to end.
- Use canary rules with known outcomes.
- Compare input, normalized, evaluated and output volumes.
- Monitor source-to-action freshness.
- Verify evaluator rule-version acknowledgements.
- Alert on checkpoint failures and stale queues.
- Track dead-letter age and volume.
- Use OpenTelemetry traces with consistent correlation context.

Infrastructure health alone does not prove that business rules are working correctly.

---

## 10. Architectural Decisions and Trade-offs

### Decision 1: Use a mature stateful stream-processing engine

**Decision:** Apache Flink for complex temporal evaluation.

**Benefits:** event-time windows, managed state, checkpoint recovery and reduced custom distributed-processing code.

**Trade-offs:** operational complexity, specialist skills and deployment overhead.

**Revisit when:** workload benchmarks, operating costs or team capability indicate a materially simpler alternative.

### Decision 2: Use a constrained declarative rule model

**Decision:** typed DSL/AST with immutable rule versions.

**Benefits:** safe business authoring, validation, auditability and predictable resource limits.

**Trade-offs:** less expressive than arbitrary code and requires engineering effort to add operators.

**Revisit when:** real use cases consistently exceed the supported model and a safe extension mechanism is justified.

### Decision 3: Decouple rule evaluation from action execution

**Decision:** durable action commands and independent workers.

**Benefits:** provider failures do not block telemetry processing; adapters and retries can evolve independently.

**Trade-offs:** asynchronous completion, extra state and idempotency complexity.

**Revisit when:** latency, volume or provider requirements materially change.

### Build versus buy

Prefer existing capabilities for Kafka, stateful processing, identity, secrets, monitoring and ticketing. Build the A Company-specific rule model, business governance and domain policy.

Evaluate total cost of ownership, support, security, staffing, reliability and migration cost—not license cost alone.

### Cloud agnosticism

Keep event contracts, the rule model, Kafka/Flink integration, OpenTelemetry and container deployment portable. Use cloud-specific services where they materially improve security, reliability or operations.

Avoid creating abstractions that add complexity without delivering meaningful portability.

---

## 11. Legacy Metadata Enrichment

### 11.1. Recommended design

Use an anti-corruption layer and a platform-owned canonical metadata read model.

```text
Legacy Monolith
      |
      v
Metadata Synchronization Worker
      |
      v
Canonical Device Metadata Store
      |
      v
Cache / Metadata Adapter
      |
      v
Telemetry Enrichment / Rule Evaluation
```

### 11.2. Implementation

1. Define a canonical metadata contract containing only the fields required by rule evaluation.
2. Synchronize updates using CDC, a supported change feed, scheduled export or incremental polling.
3. Validate updates and handle out-of-order changes.
4. Maintain metadata version and update timestamps.
5. Cache stable metadata where beneficial.
6. Define freshness requirements by field.
7. Record metadata version/effective time when an evaluation depends on it.
8. Test rule evaluation while the monolith is unavailable.
9. Gradually move ownership to a modern source of truth as the monolith is decommissioned.

The rule engine should not synchronously call the monolith for every telemetry event. Replication introduces bounded staleness but protects the high-throughput path from legacy latency and availability problems.

---

## 12. Delivery Plan and Team Structure

### Phase 0 — Discovery

- Confirm telemetry volume, peak rates, latency, retention and recovery targets.
- Identify representative rules from every division.
- Establish security and tenant-isolation requirements.
- Define event and rule contracts.
- Record the highest-risk assumptions and ADRs.

### Phase 1 — First releasable slice

Build an end-to-end path:

```text
SenML
  -> Kafka
  -> Normalizer
  -> Simple Rule Evaluation
  -> Alert Command
  -> Action Dispatcher
  -> Test Webhook / Email
  -> Audit Record
```

Include one tenant, one metric, one threshold rule, basic authoring/activation, duplicate protection and end-to-end monitoring.

### Phase 2 — Rule lifecycle and multi-tenancy

Add rule validation, simulation, approval, immutable versions, rollback, tenant authorization and configuration isolation.

### Phase 3 — Stateful CEP

Add rolling aggregates, event-time windows, late-data policies, checkpoint recovery, cooldown and threshold-crossing behavior.

### Phase 4 — Action reliability

Add provider adapters, retries, circuit breakers, dead-letter handling, reconciliation and alert lifecycle management.

### Phase 5 — Production readiness

Run scale and fault-injection tests, test recovery and disaster recovery, perform security reviews, establish on-call procedures and validate SLOs.

### Phase 6 — Metadata modernization

Deploy the metadata adapter and replicated read model, then onboard additional divisions incrementally.

### Starter playground and LLM

Use `rule-playground.html` to prototype SenML parsing, a typed rule editor, temporal aggregation, rule lifecycle, suppression and simulated action failures.

Use an LLM to accelerate test generation, UI scaffolding and documentation. Validate all generated logic through deterministic tests. Do not use an LLM as the production evaluator for deterministic threshold or temporal rules.

### Team ownership

- Platform architect/technical lead: architecture, ADRs and cross-team decisions.
- Stream-processing engineers: Kafka, Flink, state and performance.
- Backend engineers: rule management, lifecycle and tenant policy.
- Frontend engineers: rule authoring and simulation.
- Integration engineers: action adapters and metadata synchronization.
- SRE/platform engineers: deployment, security, monitoring and recovery.
- Business/product representatives: rule semantics, policy and acceptance criteria.

The shared platform team owns the engine and contracts. Divisions own their business rules and notification policies within platform guardrails.

---

## 13. Handling Conflicting Requirements

When divisions request conflicting behavior:

1. Clarify the business outcome and underlying risk.
2. Separate mandatory security/compliance constraints from preferences.
3. Identify whether the difference is policy or core platform capability.
4. Quantify alert volume, response delay, cost and operational impact.
5. Support controlled tenant-level configuration where appropriate.
6. Validate options using historical replay or representative simulations.
7. Record the decision, rationale and revisit criteria.
8. Escalate unresolved business priorities to the accountable product owner.

For example, support immediate critical alerts and grouped warning alerts using shared platform mechanisms rather than separate code branches.

**Architectural principle:** standardize the platform, allow controlled policy variation, and prevent tenant customization from bypassing security or reliability guardrails.

---

## 14. Final Recommendation

The A Company Rule Engine should be built as a shared, event-driven platform with a separate control plane, a durable telemetry backbone, a stateful CEP runtime and an asynchronous action system.

The most important design priorities are:

1. Correct temporal evaluation and recoverable state.
2. Safe, versioned, business-managed rules.
3. Strong tenant isolation and resource controls.
4. Reliable action delivery with idempotency and reconciliation.
5. Decoupling from legacy metadata dependencies.
6. Measurable SLOs, end-to-end observability and production recovery.
7. Incremental delivery driven by load testing and risk retirement.

The first production release should demonstrate one complete, observable and recoverable rule-to-action flow. The platform can then expand toward complex temporal rules, multiple divisions, richer action types and high-volume production workloads without requiring a fundamental redesign.

---
---
[Architecture](BasicArchitecture-AlertingRuleEngine.md)