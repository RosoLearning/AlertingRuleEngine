# Alerting Rule Engine
Design a centralized, flexible and scalable rule engine for real-time IoT telemetry.


# Alerting Rule Engine — Technical Case Study

# Part 1: Original Case Study

## Alerting Rule Engine

### The Scenario

A Company operates a massive IoT network, collecting telemetry data from millions of devices globally. This data is critical for monitoring device health, operational efficiency, and ensuring service level agreements (SLAs).

### The Challenge

Currently, alerting is hardcoded or managed by separate, disjointed systems. We need to build a centralized, flexible, and scalable Rule Engine to process this real-time telemetry and trigger alerts.

This engine is not just a simple filter; it's a core business-enablement tool that must also serve multiple company divisions, each with unique and independent notification requirements.

### The Task

Your goal is to architect a solution that addresses the following questions. We are interested in the decisions you make, the trade-offs you weigh, and how you would lead the solution from design to production.

Provide architectural designs, clear reasoning and answers for all questions.

### The Starter Template

We provide a small, self-contained Rule Playground (`rule-playground.html`). It simulates a device streaming IPSO/SenML telemetry and applies one naive threshold rule to it. It is deliberately minimal and serves as the entrypoint for your solution.

You are encouraged to extend it, for example with the help of an LLM, to demonstrate the concepts of your proposed solution.

**Bonus points** will be awarded for clear architectural diagrams, a good explanation of your solution, and a working demonstration built on the starter template.

---

## A Company ARCHITECTURE — TECHNICAL CASE

Design a centralized, flexible and scalable rule engine for real-time IoT telemetry.

### Core Business Requirements

#### Dynamic Rule Definition

Business users (not just developers) must be able to define, update, and activate/deactivate rules through a user interface.

#### Complex Event Processing (CEP)

The engine must evaluate not just single events (e.g., `Temperature > 80`) but also aggregates over time (e.g., `average_voltage_last_5_minutes < 10`).

#### Variety of Alert Actions

When a rule triggers, it must support diverse actions, such as triggering notifications (email, SMS), calling webhooks, creating support tickets, or initiating automated remediation actions.

#### Multi-Tenancy

The system must support multiple company divisions, allowing for isolated rule management and notification configurations per division.

### Non-Functional Requirements

#### Extensibility

The system must be designed to easily add new rule condition types (e.g., new functions, operators) and new alert action types (e.g., PagerDuty, Slack) with minimal code changes.

#### Telemetry Ingestion with IPSO / LwM2M

All raw telemetry data from IoT devices is published using the IPSO Smart Object model (LwM2M), SenML JSON encoded, and streamed into Apache Kafka.

### No Cloud Lock-in

While we currently use Azure, the solution should ideally be cloud-agnostic, leveraging open-source components and technologies where possible.

### Telemetry Data

Telemetry is published by devices using the IPSO Smart Object model (LwM2M), encoded as SenML JSON, and streamed into Apache Kafka.

#### Data Format

Each message is a SenML record list. Every measurement is addressed by its LwM2M path `ObjectID/InstanceID/ResourceID`.

Example:

```json
[
  { "bn": "urn:dev:mac:036b2738c1a4;", "bt": 1746029068 },
  { "n": "3303/0/5700", "v": 23.37 },
  { "n": "3303/0/5701", "vs": "Cel" },
  { "n": "3304/0/5700", "v": 41.78 },
  { "n": "3304/0/5701", "vs": "%" },
  { "n": "3316/0/5700", "v": 2.703 },
  { "n": "3316/0/5701", "vs": "V" },
  { "n": "3/0/9", "v": 55 },
  { "n": "4/0/2", "v": -67 }
]
```

#### Object Mapping

| Object / Path | Meaning |
|---|---|
| `3303/0/5700` | Temperature sensor value |
| `3303/0/5701` | Temperature unit |
| `3304/0/5700` | Humidity |
| `3316/0/5700` | Voltage |
| `3316/0/5701` | Voltage unit |
| `3/0/9` | Device object: battery level (%) |
| `4/0/2` | Connectivity Monitoring: radio signal strength (RSSI, dBm) |

#### Identity and Time

- `bn` (base name) identifies the device.
- `bt` (base time) is the Unix timestamp applied to all records in the message.

#### Delivery

Telemetry reaches Kafka with at-least-once semantics, so duplicates are possible.

---

## Question 1 — Architecture & Data Ingestion

### 1.1. Target Architecture

Sketch the target architecture of the Rule Engine platform, showing its main building blocks, boundaries and interactions with Kafka and other systems.

Which components would you separate, and why (e.g., by change rate, scaling profile, ownership)?

Where are the key integration points, and which contracts must be stable?

### 1.2. Telemetry Ingestion and Interpretation

How would the platform ingest and interpret the telemetry stream?

What delivery guarantees do you need end to end, and what do they cost?

How would you deal with schema evolution and devices that send unexpected or malformed data?

---

## Question 2 — Rule Model & Multi-Tenancy

### 2.1. Rule Model

How would you design the rule model so that business users can safely define rules while the platform stays evolvable?

How do you balance expressiveness against safety, testability and performance?

How would you govern the rule lifecycle (versioning, validation, approval, rollback)?

### 2.2. Multi-Tenancy

How would you architect multi-tenancy across company divisions?

What level of isolation do you choose for rules, data and notifications, and what drives that choice?

How do you prevent one division from impacting another, and how do you account for cost per division?

---

## Question 3 — Evaluation, State & Correctness

### 3.1. Processing Model

Describe the processing model of the engine at architecture level.

How do you partition work and preserve the ordering you actually need?

What consistency and delivery semantics are you targeting, and where is it acceptable to compromise?

### 3.2. Stateful Rules

Some rules depend on history (e.g., an average over the last 5 minutes).

Where does this state live, and how does it survive failures, restarts and rebalancing?

How do you handle late, out-of-order, missing or duplicated messages?

---

## Question 4 — Actions, Reliability & Operator Experience

### 4.1. Alert Action Layer

How would you design the alert action layer so it is reliable and easy to extend?

Where do you draw the boundary between rule evaluation and action execution, and why?

How do you protect the platform from slow or failing downstream systems?

### 4.2. Alert Noise

Users complain that they receive too many alerts.

What product and platform mechanisms would you consider, and what are their trade-offs?

What state does this introduce, and what does it mean for your architecture?

---

## Question 5 — Non-Functional Qualities & Operations

### 5.1. Quality Attributes and Scale

Define the key quality attributes for this platform and how your architecture achieves them.

Which service level objectives would you set, and how would you verify them?

How does the design behave under 10x telemetry volume and a growing number of complex rules? Where does it break first?

### 5.2. Security, Observability and Operations

How would you make the platform secure, observable and operable?

Consider device identity, tenant data protection and auditability of rule changes.

Which signals would you monitor, and how would you detect a silently failing rule engine?

---

## Question 6 — Decisions, Trade-offs & Delivery

### 6.1. Architectural Decisions

Which are the three most consequential architectural decisions in your design?

How would you capture and communicate them (e.g., decision records), and what would make you revisit them?

Where would you consider adopting existing products or open-source components instead of building, and how would you decide?

Are there areas where you would intentionally compromise on strict cloud-agnosticism, and why?

### 6.2. Legacy Data Enrichment

During rule evaluation, we discover a need to enrich the telemetry data with device metadata that only exists in our legacy monolithic backend. This monolith is old, hard to change, and we want to eventually decommission it.

How would you approach integrating this data into your rule engine in a way that is reliable and minimizes coupling to the legacy system, keeping our long-term goal of decommissioning it in mind?

### 6.3. Delivery Approach

How would you take this from idea to production?

How would you phase delivery, what is your first releasable slice, and which risks would you retire early?

How would you use the starter template (and tools such as an LLM) to validate your key assumptions before committing?

What team structure and ownership model would you propose for building and running the platform?

### 6.4. Conflicting Requirements

Two divisions bring conflicting requirements for the same feature. How do you handle it as the architect?

---
---

[Part 2: Executive Summary and Proposed Solution](AlertingRuleEngine.md)
---
---
[Architecture](BasicArchitecture-AlertingRuleEngine.md)