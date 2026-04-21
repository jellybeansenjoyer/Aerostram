# Phase 1 — Infrastructure Foundation

**Status:** Complete  
**Total estimate:** ~10 hours  
**Issues covered:** INFRA-1 through INFRA-6  
**Stack:** Confluent Platform 7.6.1 · KRaft combined mode · Docker Compose · Prometheus v2.51.0 · Grafana 10.4.0

---

## Table of Contents

1. [What Phase 1 Delivers](#what-phase-1-delivers)
2. [File Tree Created](#file-tree-created)
3. [INFRA-1 — Monorepo Bootstrap](#infra-1--monorepo-bootstrap)
4. [INFRA-2 — KRaft Kafka Cluster (3 Brokers)](#infra-2--kraft-kafka-cluster-3-brokers)
5. [INFRA-3 — Confluent Schema Registry](#infra-3--confluent-schema-registry)
6. [INFRA-4 — Kafka UI](#infra-4--kafka-ui)
7. [INFRA-5 — JMX Exporter + Prometheus + Grafana](#infra-5--jmx-exporter--prometheus--grafana)
8. [INFRA-6 — Topic Provisioning & Validation Suite](#infra-6--topic-provisioning--validation-suite)
9. [Startup Sequence (end to end)](#startup-sequence-end-to-end)
10. [Service Endpoints Reference](#service-endpoints-reference)
11. [Troubleshooting](#troubleshooting)

---

## What Phase 1 Delivers

Phase 1 builds the infrastructure layer that all five phases of AeroStream run on top of. By the end of it, you have:

- A **3-broker Apache Kafka cluster** running in **KRaft mode** (no ZooKeeper) with full fault tolerance — the cluster survives the loss of any single broker without dropping writes.
- A **Confluent Schema Registry** enforcing BACKWARD compatibility globally — producers can evolve schemas but can never break existing consumers.
- A **Kafka UI** (Provectus) for visual topic inspection, message browsing, and consumer group monitoring during development.
- A **JMX Prometheus exporter** attached to every broker, a **Prometheus** instance scraping them, and a **Grafana** instance with a pre-provisioned datasource — the observability baseline that all later phases build on.
- **7 Kafka topics** with deliberately chosen partition counts, replication factors, and retention/compaction policies.
- **5 shell scripts** that automate every setup step and a validation suite that confirms the entire cluster is healthy before Phase 2 begins.

---

## File Tree Created

```
aerostream/
├── .env.example                               ← committed template, no secrets
├── .gitignore                                 ← .env excluded from VCS
├── README.md                                  ← architecture overview + quick-start
├── docker-compose.yml                         ← all 7 services defined here
├── context.json                               ← machine-readable build log / decisions
│
├── infra/
│   ├── kafka/
│   │   └── kraft-config/                      ← empty, reserved for KRaft overrides
│   ├── schema-registry/                       ← empty, reserved for custom config
│   ├── prometheus/
│   │   ├── prometheus.yml                     ← scrape config (brokers + schema-registry)
│   │   └── jmx-config/
│   │       └── kafka-jmx.yml                  ← JMX metric extraction rules
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/prometheus.yml     ← Prometheus datasource (auto-provisioned)
│   │   │   └── dashboards/provider.yml        ← dashboard file provider config
│   │   └── dashboards/                        ← dashboard JSON files go here
│   └── scripts/
│       ├── init-kafka-storage.sh              ← generates CLUSTER_ID, writes to .env
│       ├── configure-schema-registry.sh       ← sets BACKWARD compatibility via REST
│       ├── download-jmx-agent.sh              ← downloads jmx_prometheus_javaagent.jar
│       ├── create-topics.sh                   ← idempotent topic provisioner (7 topics)
│       └── validate-cluster.sh               ← 11 PASS/FAIL checks, exits 0 if all pass
│
├── producer/                                  ← empty, Phase 2 Spring Boot service
├── stream-processor/                          ← empty, Phase 3 Kafka Streams service
├── ml-consumer/
│   ├── app/                                   ← empty, Phase 5 Python consumer
│   ├── models/                                ← empty, Phase 5 ML model pkl
│   ├── schemas/                               ← empty, Phase 5 Avro schemas
│   └── notebooks/                             ← empty, Phase 5 training notebook
└── docs/
    └── phase-1-infrastructure.md             ← this file
```

---

## INFRA-1 — Monorepo Bootstrap

**Priority:** Low  
**Estimate:** 1 hour  
**Purpose:** Create the foundational skeleton every future service builds on — directory layout, environment variable template, Docker Compose network, and project README. Getting this right prevents path and config drift across all five phases.

### 1.1 Directory Structure

All directories were created with a single `mkdir -p` command:

```bash
mkdir -p \
  infra/kafka/kraft-config \
  infra/schema-registry \
  infra/prometheus/jmx-config \
  infra/grafana/provisioning/datasources \
  infra/grafana/provisioning/dashboards \
  infra/grafana/dashboards \
  infra/scripts \
  producer \
  stream-processor \
  ml-consumer/app \
  ml-consumer/models \
  ml-consumer/schemas \
  ml-consumer/notebooks \
  docs
```

The service directories (`producer/`, `stream-processor/`, `ml-consumer/`) are created empty now. Later phases fill them. This ensures imports, volume mounts, and Docker build contexts never hit a "directory not found" error.

### 1.2 `.env.example`

```bash
# .env.example — committed to VCS, never contains real secrets
KAFKA_CLUSTER_ID=               # generated by init-kafka-storage.sh
KAFKA_BROKER_1_EXTERNAL_PORT=9092
KAFKA_BROKER_2_EXTERNAL_PORT=9094
KAFKA_BROKER_3_EXTERNAL_PORT=9096
SCHEMA_REGISTRY_PORT=8081
KAFKA_UI_PORT=8080
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
GRAFANA_ADMIN_PASSWORD=admin
POSTGRES_PORT=5432
POSTGRES_DB=aerostream
POSTGRES_USER=aerostream
POSTGRES_PASSWORD=changeme_in_production
```

**Why `.env.example` and not `.env`?** `.env` is the live file containing actual values and is in `.gitignore`. `.env.example` is the template that gets committed. Every developer runs `cp .env.example .env` on first clone and then fills in their own values (especially `KAFKA_CLUSTER_ID`).

Docker Compose reads `.env` automatically at startup for variable substitution. Every `${VAR:-default}` in `docker-compose.yml` has a fallback so the file works even if a variable is unset.

### 1.3 `.gitignore`

Key entries:

```gitignore
.env              # live secrets — NEVER commit
*.class           # Java compilation artefacts
target/           # Maven build output
build/ .gradle/   # Gradle build output
__pycache__/ *.pyc  # Python bytecode
.idea/ .vscode/   # IDE-specific files
*.pkl *.model     # trained ML models (large binary files)
```

The `.env` entry is the most critical. Without it a developer could accidentally commit database passwords or Kafka cluster IDs, which breaks reproducibility once shared.

### 1.4 Skeleton `docker-compose.yml`

Started with just the network and empty services/volumes block:

```yaml
networks:
  aerostream-network:
    driver: bridge

volumes: {}
services: {}
```

**Why a named bridge network?** Docker's default network assigns random IPs. A named bridge network lets every container reach every other container by its **service name** (e.g., `kafka-1:9092`, `schema-registry:8081`). No IP addresses anywhere in config.

**Validation:**
```bash
docker compose config --quiet
# → exits 0, no output (warnings suppressed)
```

### 1.5 Script Skeletons

```bash
# Create skeleton scripts with correct shebang
echo "#!/bin/bash" > infra/scripts/create-topics.sh
echo "#!/bin/bash" > infra/scripts/validate-cluster.sh

# Make executable immediately
chmod +x infra/scripts/create-topics.sh
chmod +x infra/scripts/validate-cluster.sh
```

Scripts are made executable before content is written. This prevents a common failure mode where a developer tries to run the script after INFRA-6 content is added but gets `Permission denied`.

---

## INFRA-2 — KRaft Kafka Cluster (3 Brokers)

**Priority:** Critical  
**Estimate:** 3–4 hours  
**Purpose:** The gating task. INFRA-3 through INFRA-6 cannot start until all 3 brokers report `(healthy)`. A 3-broker cluster in KRaft combined mode gives full fault tolerance — losing any one broker does not interrupt writes or reads.

### 2.1 Why KRaft (no ZooKeeper)?

Traditional Kafka required a separate ZooKeeper ensemble (typically another 3 nodes) to manage cluster metadata and leader election. KRaft (Kafka Raft) internalises that responsibility:

| Concern | ZooKeeper mode | KRaft mode |
|---------|---------------|------------|
| Metadata storage | External ZooKeeper quorum | Internal `__cluster_metadata` topic |
| Controller election | ZooKeeper ephemeral nodes | Raft consensus inside Kafka |
| Container count | 6+ (3 ZK + 3 brokers) | 3 (brokers act as controllers too) |
| Startup time | Slower (ZK must be healthy first) | Faster (no dependency) |
| Confluent support | Deprecated as of CP 7.x | Default from CP 7.x |

In **combined mode** every broker also participates in the controller quorum. This is appropriate for development and small production clusters. Separated mode (dedicated controller nodes) is for very large deployments.

### 2.2 `init-kafka-storage.sh`

KRaft requires a **cluster ID** — a UUID that is format-stored into each broker's log directory before first start. If brokers have mismatched cluster IDs they refuse to join the quorum.

```bash
#!/bin/bash
set -e

# Guard: if CLUSTER_ID is already set in .env, do nothing
EXISTING=$(grep "^KAFKA_CLUSTER_ID=" .env | cut -d= -f2)
if [ -n "$EXISTING" ]; then
  echo "KAFKA_CLUSTER_ID is already set: $EXISTING"
  exit 0
fi

# Run the Kafka storage tool inside a temporary container to generate the UUID
CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:7.6.1 \
  kafka-storage random-uuid 2>/dev/null | tr -d '\n')

# Write it back into .env using sed (idempotent — replaces existing blank value)
sed -i.bak "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=$CLUSTER_ID|" .env
rm -f .env.bak
```

Key points:
- `set -e` — any failing command aborts the script. No silent failures.
- `docker run --rm` — spins up a temporary container just to run `kafka-storage random-uuid`, then removes itself immediately. No permanent container created.
- `tr -d '\n'` — strips the trailing newline from the UUID before writing to `.env`.
- The `sed -i.bak` pattern edits `.env` in place and removes the backup (`.env.bak`) after a successful write.
- The guard at the top makes the script **idempotent** — safe to run multiple times without regenerating the UUID.

**Run it:**
```bash
bash infra/scripts/init-kafka-storage.sh
# Output:
# Generating KRaft CLUSTER_ID (requires Docker)...
# Generated CLUSTER_ID: <uuid>
# SUCCESS — KAFKA_CLUSTER_ID written to .env
```

### 2.3 Broker Configuration in `docker-compose.yml`

All three brokers use the same image and pattern. Differences between them are only `KAFKA_NODE_ID`, the external port, and the `hostname`/`container_name`. The full config for `kafka-1`:

```yaml
kafka-1:
  image: confluentinc/cp-kafka:7.6.1
  hostname: kafka-1
  container_name: kafka-1
  ports:
    - "${KAFKA_BROKER_1_EXTERNAL_PORT:-9092}:9093"   # host:container
```

**Port mapping explained:** Inside the container the broker listens on three ports (9092 PLAINTEXT, 9093 PLAINTEXT_HOST, 29093 CONTROLLER). We expose `9093` (PLAINTEXT_HOST) to the host mapped to the external port variable. This separation keeps internal cluster traffic on the Docker bridge network while clients on the host machine use the external port.

#### Environment Variables

```yaml
environment:
  KAFKA_NODE_ID: 1                      # unique integer per broker
  KAFKA_PROCESS_ROLES: broker,controller # combined mode — both roles on one node
  CLUSTER_ID: ${KAFKA_CLUSTER_ID}        # the UUID from init-kafka-storage.sh
```

`KAFKA_PROCESS_ROLES: broker,controller` is the single line that eliminates ZooKeeper. Each node participates in both the data plane (broker) and the control plane (controller Raft quorum).

```yaml
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093
```

This tells every node where to find all members of the controller quorum. The format is `nodeId@host:controllerPort`. All three brokers must agree on this value, so it is identical across all three service definitions.

```yaml
  KAFKA_LISTENERS: CONTROLLER://0.0.0.0:29093,PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:9093
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092,PLAINTEXT_HOST://localhost:9092
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
```

Three listener types serve three audiences:

| Listener | Port | Who uses it |
|----------|------|-------------|
| `CONTROLLER` | 29093 | Raft quorum traffic between brokers only |
| `PLAINTEXT` | 9092 | Internal Docker network — other containers (Schema Registry, producers) |
| `PLAINTEXT_HOST` | 9093 | External clients on the host machine (your terminal, Kafka UI on host) |

`KAFKA_ADVERTISED_LISTENERS` is what brokers tell clients to connect to. `PLAINTEXT://kafka-1:9092` works inside Docker (containers resolve `kafka-1` by hostname). `PLAINTEXT_HOST://localhost:9092` works from outside Docker.

```yaml
  KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT       # brokers talk to each other on PLAINTEXT
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER       # Raft uses CONTROLLER
```

```yaml
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3         # __consumer_offsets RF=3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3 # __transaction_state RF=3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2            # writes succeed with 2/3 brokers
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3               # default for any new topic
  KAFKA_MIN_INSYNC_REPLICAS: 2                      # producer acks=all needs 2 replicas
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"          # explicit topic control only
```

`MIN_INSYNC_REPLICAS: 2` combined with `OFFSETS_REPLICATION_FACTOR: 3` means the cluster can absorb the failure of any one broker. With 2/3 replicas in sync, `acks=all` writes still succeed.

`AUTO_CREATE_TOPICS_ENABLE: false` is a deliberate production-safety choice. Auto-creation silently creates topics with default settings (often RF=1, 1 partition). Here every topic has explicit partition counts and retention policies defined in `create-topics.sh`.

```yaml
  KAFKA_OPTS: "-javaagent:/etc/kafka/jmx-agent/jmx_prometheus_javaagent-0.19.0.jar=7071:/etc/kafka/jmx-agent/kafka-jmx.yml"
```

The JMX Prometheus Java Agent is injected via `KAFKA_OPTS`. When the JVM starts, the agent attaches to the JMX MBean server, applies the rules in `kafka-jmx.yml`, and exposes the resulting Prometheus metrics at `:7071/metrics`. This happens inside the Kafka process — no sidecar container needed.

#### Volumes and Healthcheck

```yaml
  volumes:
    - kafka-1-data:/var/lib/kafka/data         # named volume: data survives container restarts
    - ./infra/prometheus/jmx-config:/etc/kafka/jmx-agent:ro  # JMX jar + rules (read-only)
  healthcheck:
    test: kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1
    interval: 30s    # check every 30 seconds
    timeout: 10s     # fail if the command takes more than 10 seconds
    retries: 10      # broker has 30s × 10 = 300 seconds to become healthy
    start_period: 30s # don't start counting retries until 30s after container start
```

The healthcheck uses the actual Kafka CLI tool (`kafka-topics.sh`) rather than a TCP port check. A listening port does not mean the broker has completed log recovery and joined the quorum — `kafka-topics.sh --list` only succeeds once the broker is fully ready to serve requests.

`start_period: 30s` gives the JVM time to start (Kafka takes ~15–20 seconds on first boot) before health checks begin counting. Without this, the broker would be marked unhealthy during normal startup.

#### Named Volumes for Kafka Data

```yaml
volumes:
  kafka-1-data: {}
  kafka-2-data: {}
  kafka-3-data: {}
```

Named volumes persist outside the container lifecycle. `docker compose down` removes containers but not volumes. `docker compose down -v` removes both. This means broker log data survives `docker compose restart` and even `docker compose down && docker compose up`.

> **Important:** Named volumes are tied to the `CLUSTER_ID`. If you run `init-kafka-storage.sh` again (generating a new UUID) while old volumes exist, Kafka will refuse to start because the stored cluster ID in the volume won't match the new value. Remove volumes with `docker compose down -v` before regenerating the cluster ID.

---

## INFRA-3 — Confluent Schema Registry

**Priority:** High  
**Estimate:** 1 hour  
**Purpose:** Data governance layer for Avro-serialised events. The Schema Registry stores schema versions and enforces compatibility rules, preventing producers from publishing schemas that would break existing consumers.

### 3.1 Why Schema Registry?

Without Schema Registry, every consumer must know the Avro schema at compile time and bundle it in their JAR/source. This creates tight coupling: when a producer adds a new field, every consumer must be updated and redeployed simultaneously.

With Schema Registry:
- Producers register their schema once on first publish.
- Consumers fetch the schema by ID at runtime — no schema bundled in code.
- The Registry enforces compatibility rules so breaking schema changes are rejected before they reach the topic.

### 3.2 Service Definition

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.6.1
  hostname: schema-registry
  container_name: schema-registry
  ports:
    - "${SCHEMA_REGISTRY_PORT:-8081}:8081"
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry               # used in inter-instance discovery
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3   # _schemas internal topic RF=3
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: PLAINTEXT
    SCHEMA_REGISTRY_DEBUG: "true"                            # verbose logs during development
  depends_on:
    kafka-1: { condition: service_healthy }
    kafka-2: { condition: service_healthy }
    kafka-3: { condition: service_healthy }
  healthcheck:
    test: curl -f http://localhost:8081/subjects || exit 1
    interval: 15s
    timeout: 5s
    retries: 10
    start_period: 20s
```

`KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3` ensures the internal `_schemas` topic (where all registered schemas are durably stored) has RF=3. If this were left at default (1), losing a single broker would destroy the schema store and make all Avro consumers unable to deserialize messages.

`depends_on: condition: service_healthy` is the strict form. It makes Schema Registry wait for all three brokers to pass their healthcheck before starting. The simple `depends_on: [kafka-1]` only waits for the container to exist, not for Kafka to be ready.

### 3.3 `configure-schema-registry.sh`

```bash
#!/bin/bash
set -e

SR_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8081}"

# Poll until Schema Registry responds
echo "Waiting for Schema Registry at $SR_URL..."
until curl -sf "$SR_URL/subjects" > /dev/null; do
  echo "  not ready yet, retrying in 3s..."
  sleep 3
done

# Set BACKWARD compatibility globally via the REST API
RESULT=$(curl -s -X PUT "$SR_URL/config" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}')
echo "Response: $RESULT"

# Verify the setting was applied
VERIFY=$(curl -sf "$SR_URL/config")
if echo "$VERIFY" | grep -q "BACKWARD"; then
  echo "SUCCESS: Global compatibility set to BACKWARD"
else
  echo "ERROR: Failed to set compatibility. Got: $VERIFY"
  exit 1
fi
```

**Why BACKWARD compatibility?**

Confluent supports four compatibility levels:

| Level | New schema can | Old consumers reading new messages |
|-------|---------------|-------------------------------------|
| NONE | Anything | May break |
| BACKWARD | Add optional fields (with defaults) | Always work |
| FORWARD | Delete optional fields | May break (missing fields) |
| FULL | Add AND delete optional fields with defaults | Always work |

`BACKWARD` is the right default for a streaming platform: producers evolve schemas forward, consumers are pinned to their deployed version. A producer that adds a new field with a default value is harmless — an old consumer simply ignores the unknown field. If a producer tries to remove a required field or change a type, Schema Registry rejects the registration before it reaches the topic.

The PUT request sets this globally across all subjects. Individual subjects can override it if needed.

**Run it:**
```bash
bash infra/scripts/configure-schema-registry.sh
# Output:
# Schema Registry is up. Setting BACKWARD compatibility...
# Response: {"compatibility":"BACKWARD"}
# SUCCESS: Global compatibility set to BACKWARD
```

**Verify manually:**
```bash
curl http://localhost:8081/config
# {"compatibilityLevel":"BACKWARD"}
```

---

## INFRA-4 — Kafka UI

**Priority:** Medium  
**Estimate:** 30 minutes  
**Purpose:** A browser-based interface for cluster introspection during development. Removes the need to memorise `kafka-topics.sh` flags for every inspection task. Particularly useful in Phases 2–5 when debugging message flow across topics.

### 4.1 Service Definition

```yaml
kafka-ui:
  image: provectuslabs/kafka-ui:latest
  container_name: kafka-ui
  ports:
    - "${KAFKA_UI_PORT:-8080}:8080"
  environment:
    KAFKA_CLUSTERS_0_NAME: aerostream-local          # display name in the UI
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
    KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081  # enables Avro deserialization
    KAFKA_CLUSTERS_0_METRICS_PORT: 7071              # JMX exporter port (from INFRA-5)
    DYNAMIC_CONFIG_ENABLED: "true"                   # allows config changes via the UI at runtime
    AUTH_TYPE: "disabled"                            # no login prompt in development
  depends_on:
    - kafka-1
    - kafka-2
    - kafka-3
    - schema-registry
  networks:
    - aerostream-network
  restart: unless-stopped
```

`KAFKA_CLUSTERS_0_SCHEMAREGISTRY` connects the UI to Schema Registry so that when you browse messages on Avro topics (like `raw-telemetry` in Phase 2), they are displayed as human-readable JSON rather than raw binary.

`KAFKA_CLUSTERS_0_METRICS_PORT: 7071` pre-wires the metrics connection to the JMX exporter that is added in INFRA-5. The UI can display JMX metrics per broker once that exporter is running.

`restart: unless-stopped` ensures Kafka UI comes back automatically after a `docker compose restart` or system reboot, without requiring a full `docker compose up`.

**Access:** `http://localhost:8080`

---

## INFRA-5 — JMX Exporter + Prometheus + Grafana

**Priority:** High  
**Estimate:** 2.5 hours  
**Purpose:** Attach Prometheus-compatible metrics to every Kafka broker using the JMX Java agent. Deploy Prometheus to collect them and Grafana to visualise them. Consumer lag dashboards in Phase 5 build directly on this infrastructure.

### 5.1 How the JMX Agent Works

Kafka exposes hundreds of internal metrics via JMX (Java Management Extensions) — MBean objects with names like `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec`. The `jmx_prometheus_javaagent` attaches to the JVM at startup, reads those MBeans, applies transformation rules, and serves the result as Prometheus `/metrics` on a configurable port.

This is a **zero-sidecar** approach. No separate exporter container is needed — the agent runs inside the Kafka process itself.

### 5.2 Downloading the Agent JAR

The JAR must be present on disk before `docker compose up` runs, because it is bind-mounted into the broker containers:

```bash
# infra/scripts/download-jmx-agent.sh
JAR_VERSION="0.19.0"
JAR_NAME="jmx_prometheus_javaagent-${JAR_VERSION}.jar"
DEST="infra/prometheus/jmx-config/${JAR_NAME}"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JAR_VERSION}/${JAR_NAME}"

curl -L -o "$DEST" "$URL"
```

**Run it once, before the cluster starts:**
```bash
bash infra/scripts/download-jmx-agent.sh
# Downloaded: infra/prometheus/jmx-config/jmx_prometheus_javaagent-0.19.0.jar (367K)
```

### 5.3 `kafka-jmx.yml` — Metric Extraction Rules

```yaml
# infra/prometheus/jmx-config/kafka-jmx.yml
lowercaseOutputName: true        # kafka_server_... not Kafka_Server_...
lowercaseOutputLabelNames: true

rules:
  # Throughput per topic: MessagesInPerSec, BytesInPerSec, BytesOutPerSec
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+), topic=(.+)><>Count
    name: kafka_server_broker_topic_$1_total
    labels:
      topic: "$2"

  # Cluster-wide throughput (no topic label)
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+)><>Count
    name: kafka_server_broker_$1_total

  # Replication health: LeaderCount, UnderReplicatedPartitions, OfflinePartitionsCount
  - pattern: kafka.server<type=ReplicaManager, name=(.+)><>Value
    name: kafka_server_replica_manager_$1

  # Controller: ActiveControllerCount (should be 1 across the cluster)
  - pattern: kafka.controller<type=KafkaController, name=(.+)><>Value
    name: kafka_controller_$1

  # Request rates by type: Produce, Fetch, Metadata
  - pattern: kafka.network<type=RequestMetrics, name=RequestsPerSec, request=(.+)><>Count
    name: kafka_network_requests_total
    labels:
      request: "$1"

  # Handler thread utilisation — alerts if this drops below 20%
  - pattern: kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>OneMinuteRate
    name: kafka_server_request_handler_avg_idle_percent

  # Log flush rate
  - pattern: kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>Count
    name: kafka_log_flush_rate_total

  # Catch-all: export any remaining JMX attributes not matched above
  - pattern: ".*"
```

Each rule maps a JMX MBean ObjectName pattern to a Prometheus metric name. `$1`, `$2` are capture groups from the regex. The catch-all `".*"` at the end ensures no metric is silently dropped.

### 5.4 Attaching the Agent to Kafka Brokers

The agent is injected via `KAFKA_OPTS` in the broker's environment:

```yaml
# Added to all 3 broker services in docker-compose.yml
KAFKA_OPTS: "-javaagent:/etc/kafka/jmx-agent/jmx_prometheus_javaagent-0.19.0.jar=7071:/etc/kafka/jmx-agent/kafka-jmx.yml"
expose:
  - "7071"     # internal only — Prometheus scrapes this; no host port mapping needed
volumes:
  - ./infra/prometheus/jmx-config:/etc/kafka/jmx-agent:ro  # mounts jar + rules file
```

The javaagent argument format is: `-javaagent:<path_to_jar>=<port>:<path_to_config>`. Here:
- Path to JAR: `/etc/kafka/jmx-agent/jmx_prometheus_javaagent-0.19.0.jar`
- Metrics port: `7071`
- Config file: `/etc/kafka/jmx-agent/kafka-jmx.yml`

`expose: ["7071"]` makes port 7071 accessible to other containers on `aerostream-network` (Prometheus) without publishing it to the host. Prometheus runs inside Docker and accesses `kafka-1:7071` directly.

### 5.5 `prometheus.yml` — Scrape Configuration

```yaml
# infra/prometheus/prometheus.yml
global:
  scrape_interval: 15s       # pull metrics every 15 seconds
  evaluation_interval: 15s   # evaluate alerting rules every 15 seconds

scrape_configs:
  - job_name: kafka-brokers
    static_configs:
      - targets:
          - kafka-1:7071     # resolves to the JMX exporter inside kafka-1
          - kafka-2:7071
          - kafka-3:7071
        labels:
          cluster: aerostream-local  # added to every metric for multi-cluster support

  - job_name: schema-registry
    metrics_path: /metrics
    static_configs:
      - targets:
          - schema-registry:8081
```

`scrape_interval: 15s` matches the Kafka JMX reporting interval, so you get fresh data on every scrape without polling faster than data updates.

The `cluster: aerostream-local` label is added to every scraped metric, making it easy to filter in Grafana when you later have multiple environments.

### 5.6 Prometheus Service

```yaml
prometheus:
  image: prom/prometheus:v2.51.0
  container_name: prometheus
  ports:
    - "${PROMETHEUS_PORT:-9090}:9090"
  volumes:
    - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    - ./infra/prometheus/jmx-config:/etc/kafka/jmx-agent:ro  # same rules for consistency
  command:
    - --config.file=/etc/prometheus/prometheus.yml
    - --storage.tsdb.retention.time=15d    # keep 15 days of metrics history
    - --web.enable-lifecycle               # allows hot-reload via POST /-/reload
  depends_on:
    - kafka-1
    - kafka-2
    - kafka-3
  networks:
    - aerostream-network
```

`--web.enable-lifecycle` is important for development: after changing `prometheus.yml`, you can reload the config without restarting the container:
```bash
curl -X POST http://localhost:9090/-/reload
```

### 5.7 Grafana Provisioning

Grafana supports **file-based provisioning** — datasources and dashboard providers can be pre-configured by mounting YAML files into `/etc/grafana/provisioning/`. This eliminates manual UI configuration on every fresh start.

**Datasource (`infra/grafana/provisioning/datasources/prometheus.yml`):**
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy         # Grafana proxies requests to Prometheus (not browser-direct)
    url: http://prometheus:9090
    isDefault: true       # default datasource for all new panels
    jsonData:
      timeInterval: 15s   # hint to Grafana to use 15s as the minimum interval
```

`access: proxy` means the browser sends queries to Grafana's backend, which forwards them to Prometheus. This works even if Prometheus is not accessible from the browser's network — only Grafana needs to reach Prometheus (which it can via `aerostream-network`).

**Dashboard provider (`infra/grafana/provisioning/dashboards/provider.yml`):**
```yaml
apiVersion: 1
providers:
  - name: AeroStream
    folder: AeroStream    # dashboards appear in the "AeroStream" folder in Grafana
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30   # re-scans dashboards/ every 30 seconds for new JSON files
    allowUiUpdates: true        # changes made in UI are saved back to JSON files
    options:
      path: /var/lib/grafana/dashboards
```

Any `.json` file dropped into `infra/grafana/dashboards/` is automatically loaded into Grafana within 30 seconds. In Phase 5 this is how the consumer lag heatmap and tire degradation dashboards are provisioned.

**Grafana service:**
```yaml
grafana:
  image: grafana/grafana:10.4.0
  container_name: grafana
  ports:
    - "${GRAFANA_PORT:-3000}:3000"
  environment:
    GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
    GF_USERS_ALLOW_SIGN_UP: "false"   # prevents anonymous sign-up
  volumes:
    - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro
    - ./infra/grafana/dashboards:/var/lib/grafana/dashboards:ro
    - grafana-data:/var/lib/grafana    # persists dashboard edits and users
  depends_on:
    - prometheus
  networks:
    - aerostream-network
```

`grafana-data` is a named volume that persists user-created dashboards, saved queries, and alert configurations across container restarts.

---

## INFRA-6 — Topic Provisioning & Validation Suite

**Priority:** High  
**Estimate:** 2 hours  
**Purpose:** Idempotently create all 7 AeroStream topics with precise configs. Then validate the entire Phase 1 cluster programmatically — every check must pass before Phase 2 starts.

### 6.1 Topic Design Decisions

| Topic | Partitions | RF | Retention | Policy | Rationale |
|-------|-----------|-----|-----------|--------|-----------|
| `raw-telemetry` | 20 | 3 | 2h | delete | 20 cars, 1 partition per car for key-ordered processing. Short retention (high volume, downstream processes quickly). |
| `enriched-telemetry` | 20 | 3 | 6h | delete | Matches raw partition count. 6h gives Phase 5 ML consumer time to catch up after a brief outage. |
| `stream-aggregates` | 10 | 3 | 24h | delete | Kafka Streams aggregate output — lower throughput than raw, 10 partitions sufficient. |
| `pit-predictions` | 5 | 3 | 24h | delete | ML consumer output — lower throughput. 5 partitions. |
| `race-outcomes` | 5 | 3 | ∞ | compact | Final result per car keyed by `carId`. Compaction keeps only the latest value per key. |
| `dlq-telemetry` | 3 | 3 | 7d | delete | Dead letter queue. 7-day retention gives engineers a week to investigate deserialization failures. |
| `circuit-metadata` | 3 | 3 | ∞ | compact | Reference data keyed by `circuitId`. Compaction ensures the latest metadata is always available. |

**Compacted vs delete:** `cleanup.policy=delete` (the default) removes messages older than `retention.ms`. `cleanup.policy=compact` removes duplicate keys, keeping only the most recent value for each key. Compacted topics are used as materialised views — a new consumer can read the entire topic and reconstruct current state without needing full history.

### 6.2 `create-topics.sh`

```bash
#!/bin/bash
set -e

BROKER="kafka-1:9092"
CONTAINER="kafka-1"

# Wait for the broker to be ready before attempting topic creation
echo "Waiting for Kafka broker..."
until docker exec $CONTAINER kafka-topics.sh --bootstrap-server $BROKER --list &>/dev/null; do
  echo "  broker not ready, retrying in 3s..."
  sleep 3
done
```

The polling loop runs `kafka-topics.sh --list` inside the `kafka-1` container. `&>/dev/null` silences all output (both stdout and stderr). The `until` loop retries every 3 seconds until the command exits 0 — meaning the broker is fully ready.

```bash
# Helper function — avoids repeating --bootstrap-server and --create flags
create_topic() {
  local NAME=$1; shift          # first arg is the topic name; shift removes it
  printf "  %-25s" "$NAME"       # left-aligned, 25-char wide column for the name
  docker exec $CONTAINER kafka-topics.sh \
    --bootstrap-server $BROKER \
    --create --if-not-exists \   # idempotent: silently skips if topic already exists
    --topic "$NAME" "$@" > /dev/null 2>&1 \
    && echo "CREATED" || echo "EXISTS (skipped)"
}
```

`--if-not-exists` is the key to idempotency. Running this script on a cluster that already has the topics simply prints `EXISTS (skipped)` for each one. No error, no duplicate, no state corruption. This matters because `set -e` would abort the script on the first non-zero exit code — `--if-not-exists` ensures that exit code is always 0.

**Topic creation calls:**
```bash
# High-throughput telemetry — 20 partitions each, 2/3 min-ISR
create_topic raw-telemetry \
  --partitions 20 --replication-factor 3 \
  --config retention.ms=7200000 \       # 2 hours
  --config min.insync.replicas=2

create_topic enriched-telemetry \
  --partitions 20 --replication-factor 3 \
  --config retention.ms=21600000 \      # 6 hours
  --config min.insync.replicas=2

# Aggregate and prediction topics — lower partition counts
create_topic stream-aggregates --partitions 10 --replication-factor 3 --config retention.ms=86400000
create_topic pit-predictions   --partitions 5  --replication-factor 3 --config retention.ms=86400000

# Compacted reference topics — no time-based deletion
create_topic race-outcomes \
  --partitions 5 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1  # compact more aggressively (default is 0.5)

create_topic circuit-metadata --partitions 3 --replication-factor 3 --config cleanup.policy=compact

# Dead letter queue — 7-day retention for post-mortem debugging
create_topic dlq-telemetry --partitions 3 --replication-factor 3 --config retention.ms=604800000
```

**Run it:**
```bash
bash infra/scripts/create-topics.sh
# Output:
#   raw-telemetry             CREATED
#   enriched-telemetry        CREATED
#   stream-aggregates         CREATED
#   pit-predictions           CREATED
#   race-outcomes             CREATED
#   circuit-metadata          CREATED
#   dlq-telemetry             CREATED
#
# ALL TOPICS CREATED. Current topic list:
# circuit-metadata
# dlq-telemetry
# enriched-telemetry
# pit-predictions
# race-outcomes
# raw-telemetry
# stream-aggregates
```

### 6.3 `validate-cluster.sh`

The validation script runs 11 programmatic checks and prints a PASS/FAIL table. It exits `0` only when every check passes — this makes it usable as a CI gate.

```bash
#!/bin/bash
PASS=0; FAIL=0

check() {
  local DESC="$1"
  local CMD="$2"
  printf "  %-55s" "$DESC"
  if eval "$CMD" &>/dev/null; then
    echo "[ PASS ]"
    ((PASS++))
  else
    echo "[ FAIL ]"
    ((FAIL++))
  fi
}
```

The `check` function takes a description and a shell command string. It runs the command via `eval`, discards all output (`&>/dev/null`), and increments either `PASS` or `FAIL` based on the exit code. The description is printed in a 55-character fixed-width column for alignment.

**Broker checks:**
```bash
check "kafka-1 broker reachable" \
  "docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092"
```

`kafka-broker-api-versions.sh` is a stronger check than a TCP connection: it actually negotiates with the broker over the Kafka protocol and lists supported API versions. If this exits 0, the broker is fully operational.

**ZooKeeper absence check:**
```bash
check "No ZooKeeper containers running" \
  "! docker ps --format '{{.Names}}' | grep -qi zookeeper"
```

The `!` prefix inverts the exit code. The check passes (exit 0) when `grep` finds no ZooKeeper container names — i.e., when the cluster is correctly running in ZooKeeper-free KRaft mode.

**Topic integrity checks:**
```bash
check "All 7 topics exist" \
  "[ \$(docker exec kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --list | grep -v '^__' | wc -l) -ge 7 ]"
```

`grep -v '^__'` filters out internal topics like `__consumer_offsets` and `__transaction_state`. The `wc -l` count must be at least 7.

```bash
check "All topics have ReplicationFactor=3" \
  "[ \$(docker exec kafka-1 kafka-topics.sh ... --describe | grep -v '^__' | grep 'ReplicationFactor' | grep -v 'ReplicationFactor: 3' | wc -l) -eq 0 ]"
```

This double-grep pattern: find all `ReplicationFactor` lines, then filter out the ones that say `3`. If any remain, the count is non-zero and the check fails. A count of 0 means every topic has RF=3.

**Schema Registry checks:**
```bash
check "Schema Registry HTTP 200 on /subjects" \
  "curl -sf http://localhost:${SCHEMA_REGISTRY_PORT:-8081}/subjects"

check "Schema Registry BACKWARD compatibility" \
  "curl -sf http://localhost:${SCHEMA_REGISTRY_PORT:-8081}/config | grep -q BACKWARD"
```

`curl -sf` fails with a non-zero exit code on HTTP errors (`-f`) and suppresses progress output (`-s`). The second check pipes the JSON response into `grep -q BACKWARD` — only passes if the word "BACKWARD" appears in the response body.

**Observability checks:**
```bash
check "Prometheus is accessible" \
  "curl -sf http://localhost:${PROMETHEUS_PORT:-9090}/-/healthy"

check "Grafana is accessible" \
  "curl -sf http://localhost:${GRAFANA_PORT:-3000}/api/health | grep -q 'ok'"
```

Prometheus's `/-/healthy` endpoint returns HTTP 200 when it is ready to serve requests. Grafana's `/api/health` returns `{"database":"ok","version":"..."}` — `grep -q 'ok'` confirms the database is healthy.

**Summary block:**
```bash
if [ $FAIL -eq 0 ]; then
  echo "ALL CHECKS PASSED — Phase 1 complete. Proceed to Phase 2."
  exit 0
else
  echo "$FAIL CHECK(S) FAILED — fix issues above before proceeding."
  exit 1
fi
```

Exit code `1` on any failure makes this script compatible with CI pipelines. A `bash infra/scripts/validate-cluster.sh && echo "ready"` in a CI step will only proceed if every check passes.

**Expected output on a healthy cluster:**
```
======================================================
  AeroStream — Phase 1 Validation Suite
======================================================

[ Kafka Cluster ]
  kafka-1 broker reachable                               [ PASS ]
  kafka-2 broker reachable                               [ PASS ]
  kafka-3 broker reachable                               [ PASS ]
  No ZooKeeper containers running                        [ PASS ]

[ Topics ]
  All 7 topics exist                                     [ PASS ]
  raw-telemetry has 20 partitions                        [ PASS ]
  All topics have ReplicationFactor=3                    [ PASS ]
  race-outcomes uses compact cleanup policy              [ PASS ]
  circuit-metadata uses compact cleanup policy           [ PASS ]

[ Schema Registry ]
  Schema Registry HTTP 200 on /subjects                  [ PASS ]
  Schema Registry BACKWARD compatibility                 [ PASS ]

[ Observability ]
  Prometheus is accessible                               [ PASS ]
  Grafana is accessible                                  [ PASS ]

======================================================
  Results: 13 passed, 0 failed
======================================================
  ALL CHECKS PASSED — Phase 1 complete. Proceed to Phase 2.
```

---

## Startup Sequence (End to End)

This is the complete, ordered sequence of commands to bring Phase 1 up from a fresh clone:

```bash
# ── Step 1: Copy environment template ─────────────────────────────────────────
cp .env.example .env
# Creates your local .env file. Fill in POSTGRES_PASSWORD if needed.
# KAFKA_CLUSTER_ID is left blank — the next script fills it.

# ── Step 2: Download JMX Prometheus agent JAR ──────────────────────────────────
bash infra/scripts/download-jmx-agent.sh
# Downloads jmx_prometheus_javaagent-0.19.0.jar into infra/prometheus/jmx-config/
# Must be done before docker compose up — the jar is bind-mounted into brokers.

# ── Step 3: Generate KRaft Cluster ID ─────────────────────────────────────────
bash infra/scripts/init-kafka-storage.sh
# Spins up a temporary cp-kafka container, generates a UUID, writes it to .env.
# Safe to run multiple times — skips if KAFKA_CLUSTER_ID is already set.

# ── Step 4: Start all services ─────────────────────────────────────────────────
docker compose up -d
# Starts: kafka-1, kafka-2, kafka-3, schema-registry, kafka-ui, prometheus, grafana
# Brokers take ~30–60 seconds to become healthy.
# Watch progress: docker compose ps

# ── Step 5: Wait for brokers ──────────────────────────────────────────────────
docker compose ps
# Wait until kafka-1, kafka-2, kafka-3 all show (healthy) in the STATUS column.
# If a broker shows (unhealthy) after 5 minutes, check logs:
# docker logs kafka-1

# ── Step 6: Create Kafka topics ────────────────────────────────────────────────
bash infra/scripts/create-topics.sh
# Creates all 7 topics with correct partition counts, RF, and retention configs.
# Idempotent — safe to run multiple times.

# ── Step 7: Configure Schema Registry compatibility ───────────────────────────
bash infra/scripts/configure-schema-registry.sh
# Polls until Schema Registry is up, then sets global BACKWARD compatibility.

# ── Step 8: Validate the cluster ──────────────────────────────────────────────
bash infra/scripts/validate-cluster.sh
# Must print ALL CHECKS PASSED and exit 0 before Phase 2 begins.
```

**Total startup time:** ~3–5 minutes from `docker compose up -d` to all services healthy.

---

## Service Endpoints Reference

| Service | URL / Host:Port | Purpose |
|---------|-----------------|---------|
| Kafka Broker 1 | `localhost:9092` | Producer/consumer connections from host |
| Kafka Broker 2 | `localhost:9094` | Producer/consumer connections from host |
| Kafka Broker 3 | `localhost:9096` | Producer/consumer connections from host |
| Schema Registry | `http://localhost:8081` | Schema registration and retrieval |
| Kafka UI | `http://localhost:8080` | Visual cluster management |
| Prometheus | `http://localhost:9090` | Metrics query and targets |
| Grafana | `http://localhost:3000` | Dashboards (admin / admin) |
| JMX metrics (per broker) | `kafka-1:7071` | Internal only — Prometheus scrapes this |

---

## Troubleshooting

**Brokers stuck in `(health: starting)` indefinitely**

The most common cause is a missing or wrong `KAFKA_CLUSTER_ID`.

```bash
# Check what's in .env
grep KAFKA_CLUSTER_ID .env

# If blank: run the init script
bash infra/scripts/init-kafka-storage.sh

# If set but wrong (e.g., old volumes from a different cluster):
docker compose down -v   # removes containers AND named volumes
bash infra/scripts/init-kafka-storage.sh
docker compose up -d
```

**`kafka-1` logs show `InconsistentClusterIdException`**

The stored cluster ID in the named volume doesn't match the value in `.env`. Remove the volumes and restart:

```bash
docker compose down -v
bash infra/scripts/init-kafka-storage.sh
docker compose up -d
```

**`validate-cluster.sh` fails on `All topics have ReplicationFactor=3`**

Topics were created before the cluster reached quorum (all 3 brokers healthy). Drop and recreate:

```bash
# Inside kafka-1 — delete all user topics
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --delete --topic raw-telemetry
# Repeat for all 7 topics, then:
bash infra/scripts/create-topics.sh
```

**Prometheus targets show `DOWN` for kafka brokers**

The JMX agent JAR is missing from `infra/prometheus/jmx-config/`. The mount exists but the file doesn't.

```bash
ls infra/prometheus/jmx-config/
# If jmx_prometheus_javaagent-0.19.0.jar is absent:
bash infra/scripts/download-jmx-agent.sh
docker compose restart kafka-1 kafka-2 kafka-3
```

**Grafana datasource shows `Bad gateway`**

Prometheus is not yet running or is still starting. Prometheus depends on the Kafka brokers — if they're slow to start, Prometheus delays too.

```bash
docker compose ps prometheus    # check status
docker logs prometheus          # check for config errors
curl http://localhost:9090/-/healthy  # should return "Prometheus Server is Healthy."
```
