# Phase 2 — Telemetry Simulator & Avro Producer

**Status:** Complete  
**Total estimate:** ~12 hours  
**Issues covered:** PROD-1 through PROD-6  
**Stack:** Java 21 · Spring Boot 3.2.5 · Apache Avro 1.11.3 · Confluent Kafka Avro Serializer 7.6.1 · Maven 3.9 · Micrometer · Prometheus Actuator

---

## Table of Contents

1. [What Phase 2 Delivers](#what-phase-2-delivers)
2. [File Tree Created](#file-tree-created)
3. [PROD-1 — Spring Boot Project Bootstrap](#prod-1--spring-boot-project-bootstrap)
4. [PROD-2 — Avro Schema: TelemetryEvent](#prod-2--avro-schema-telemetryevent)
5. [PROD-3 — Kafka Producer Configuration](#prod-3--kafka-producer-configuration)
6. [PROD-4 — Telemetry Simulation Engine](#prod-4--telemetry-simulation-engine)
7. [PROD-5 — Dead Letter Queue (DLQ)](#prod-5--dead-letter-queue-dlq)
8. [PROD-6 — Micrometer Metrics & Observability](#prod-6--micrometer-metrics--observability)
9. [How It All Connects: The Data Flow](#how-it-all-connects-the-data-flow)
10. [Phase 2 Startup Sequence](#phase-2-startup-sequence)
11. [API Reference](#api-reference)
12. [Definition of Done Verification](#definition-of-done-verification)
13. [Key Design Decisions Summary](#key-design-decisions-summary)

---

## What Phase 2 Delivers

Phase 1 gave us a running Kafka cluster with three brokers, Schema Registry, and Prometheus. Phase 2 produces data — it fills the pipeline.

By the end of Phase 2 you have:

- A **Spring Boot 3.2.5 service** (`producer/`) that compiles with Maven, builds into a Docker image, and starts as a container alongside the Phase 1 cluster.
- A **canonical 26-field Avro schema** (`TelemetryEvent`) that is the data contract for the entire pipeline. Every downstream service (Phase 3 Kafka Streams, Phase 5 ML consumer) reads data whose structure is defined here.
- A **production-grade idempotent Kafka producer** with LZ4 compression, 64 KB batching, and a custom partitioner that guarantees all events for a given car land on the same partition — enabling ordered stream processing in Phase 3.
- A **realistic F1 telemetry simulation engine** that drives 20 cars around a virtual circuit, producing physics-accurate sensor readings: tire temperatures that heat up and cool down, compound-dependent tire wear, fuel burn, g-forces, ERS deployment, and DRS activation.
- A **Dead Letter Queue (DLQ)** implementation that captures producer failures with full diagnostic headers so no event is silently lost.
- **Micrometer counters and gauges** hooked into Prometheus and surfaced through a Grafana dashboard, giving live visibility into events/sec, DLQ rate, and active car count.

---

## File Tree Created

```
producer/
├── .gitignore                                    ← excludes target/ and generated sources from VCS
├── Dockerfile                                    ← multi-stage build (Maven builder → Alpine runtime)
├── pom.xml                                       ← Spring Boot 3.2.5, Java 21, Avro + Confluent deps
│
└── src/
    ├── main/
    │   ├── avro/
    │   │   └── TelemetryEvent.avsc               ← 26-field Avro schema (data contract)
    │   ├── resources/
    │   │   ├── application.yml                   ← local dev config (localhost brokers)
    │   │   └── application-docker.yml            ← Docker overrides (service-name brokers)
    │   └── java/com/aerostream/
    │       ├── AerostreamProducerApplication.java ← @SpringBootApplication main class
    │       ├── config/
    │       │   ├── KafkaProducerConfig.java       ← idempotent producer factory + KafkaTemplate
    │       │   └── SchemaRegistryConfig.java      ← CachedSchemaRegistryClient bean
    │       ├── kafka/
    │       │   ├── TelemetryPartitioner.java      ← car_id → partition (deterministic hash)
    │       │   └── DlqPublisher.java              ← routes failures to dlq-telemetry
    │       ├── model/
    │       │   └── CarState.java                  ← mutable physics state per car
    │       ├── simulation/
    │       │   ├── PhysicsEngine.java             ← tick() advances all sensor values
    │       │   ├── TelemetryEventMapper.java      ← CarState → TelemetryEvent Avro object
    │       │   ├── TelemetrySimulator.java        ← ScheduledExecutorService, 20 cars, start/stop
    │       │   ├── SimulatorProperties.java       ← @ConfigurationProperties(prefix=simulator)
    │       │   ├── SimulatorStatus.java           ← status DTO returned by /api/simulator/status
    │       │   └── SimulatorController.java       ← REST controller for simulator lifecycle
    │       └── testing/
    │           ├── PoisonPillGenerator.java       ← generates deliberately invalid events
    │           └── TestController.java            ← POST /api/test/poison-pill (non-production)
    └── test/
        └── java/com/aerostream/
            ├── schema/
            │   └── SchemaRegistrationTest.java   ← validates Avro schema structure
            └── kafka/
                └── TelemetryPartitionerTest.java ← stability + bounds tests for partitioner

infra/
├── prometheus/
│   └── prometheus.yml                            ← added aerostream-producer scrape job (PROD-6)
└── grafana/
    └── dashboards/
        └── producer-dashboard.json               ← 7-panel Grafana dashboard (PROD-6)
```

**Also modified:**
- `docker-compose.yml` — `producer` service added
- `.env.example` — `PRODUCER_PORT` and `SIMULATOR_EVENTS_PER_SECOND` added

---

## PROD-1 — Spring Boot Project Bootstrap

**Priority:** Critical  
**Estimate:** 1.5 hours  
**Purpose:** Create the compilable, runnable skeleton of the producer service. Every other PROD issue adds a class or config on top of this foundation. Getting the build and Docker integration working first means you can validate each subsequent issue in a running container.

### 1.1 Why Spring Boot 3.2.x?

Spring Boot 3.x requires Java 17 as a minimum and Java 21 as the recommended runtime. We chose 3.2.5 (the latest patch release of the 3.2 line at the time) for several reasons:

- **Java 21 virtual threads** are available (via `@EnableAsync` with virtual thread executor if needed later).
- **Native Kafka support** — `spring-kafka` is a first-class starter, no manual Kafka client wiring needed.
- **Actuator + Micrometer** ship as a single `spring-boot-starter-actuator` dependency and expose Prometheus metrics out of the box.
- **Spring Boot parent POM** manages all transitive dependency versions, drastically reducing `pom.xml` maintenance.

### 1.2 `pom.xml` — The Build File

```xml
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>3.2.5</version>
</parent>
```

The Spring Boot parent POM is a parent project in Maven that pre-configures:
- Plugin versions (maven-compiler-plugin at Java 21, spring-boot-maven-plugin)
- Dependency versions for all Spring ecosystem libraries
- Default build lifecycle bindings

This means you do not write `<version>` on Spring-managed dependencies like `spring-boot-starter-web` or `spring-kafka` — Spring Boot picks compatible versions automatically.

```xml
<properties>
  <java.version>21</java.version>
  <avro.version>1.11.3</avro.version>
  <confluent.version>7.6.1</confluent.version>
</properties>
```

The Confluent version (7.6.1) must match the Kafka cluster version from Phase 1. Mismatched versions cause `UnsupportedVersionException` at runtime when the producer tries to connect.

**Key dependencies and why each exists:**

| Dependency | Why |
|-----------|-----|
| `spring-boot-starter-web` | Exposes the REST API (`/api/simulator/start`, `/api/test/poison-pill`) |
| `spring-kafka` | High-level `KafkaTemplate` abstraction and `ProducerFactory` management |
| `kafka-avro-serializer` (Confluent) | Serializes `TelemetryEvent` Avro objects to bytes AND registers the schema in Schema Registry automatically on first publish |
| `kafka-schema-registry-client` (Confluent) | `CachedSchemaRegistryClient` — avoids hitting Schema Registry HTTP for every message by caching schema IDs in memory |
| `avro` (Apache) | Avro runtime library — provides `GenericRecord`, schema parsing, binary encoding |
| `spring-boot-starter-actuator` | Exposes `/actuator/health` and `/actuator/prometheus` endpoints |
| `micrometer-registry-prometheus` | Translates Micrometer metrics to Prometheus text format at `/actuator/prometheus` |
| `lombok` | Reduces boilerplate: `@Data`, `@Builder`, `@Slf4j`, `@RequiredArgsConstructor` |

**The Confluent Maven repository** must be explicitly declared because `kafka-avro-serializer` is not published to Maven Central:

```xml
<repositories>
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>
```

Without this block, `mvn clean package` fails with `Could not find artifact io.confluent:kafka-avro-serializer`.

**The `avro-maven-plugin`** is what makes Avro code generation automatic:

```xml
<plugin>
  <groupId>org.apache.avro</groupId>
  <artifactId>avro-maven-plugin</artifactId>
  <version>${avro.version}</version>
  <executions>
    <execution>
      <phase>generate-sources</phase>
      <goals><goal>schema</goal></goals>
      <configuration>
        <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
        <outputDirectory>${project.build.directory}/generated-sources/avro</outputDirectory>
        <stringType>String</stringType>
      </configuration>
    </execution>
  </executions>
</plugin>
```

This plugin runs during the `generate-sources` Maven lifecycle phase — before compilation. It reads every `.avsc` file in `src/main/avro/` and generates corresponding Java classes in `target/generated-sources/avro/`. Your Java code can then `import com.aerostream.avro.TelemetryEvent` as if it were a hand-written class. The `<stringType>String</stringType>` option generates `String` fields instead of the Avro-specific `CharSequence` type, which is cleaner to work with in Java.

### 1.3 `AerostreamProducerApplication.java` — The Main Class

```java
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SimulatorProperties.class)
public class AerostreamProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(AerostreamProducerApplication.class, args);
    }
}
```

Three annotations, each doing important work:

- **`@SpringBootApplication`** — shorthand for `@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan`. It tells Spring Boot to scan the `com.aerostream` package for all `@Component`, `@Service`, `@Repository`, `@Controller`, and `@Configuration` classes and wire them automatically.
- **`@EnableScheduling`** — activates Spring's scheduled task executor. Without this, `@Scheduled` annotations elsewhere would be silently ignored.
- **`@EnableConfigurationProperties(SimulatorProperties.class)`** — registers the `SimulatorProperties` class so Spring can bind `application.yml` values under the `simulator:` prefix to it (PROD-4 uses this extensively).

### 1.4 `application.yml` vs `application-docker.yml`

Spring Boot supports **profile-specific configuration files**. A file named `application-docker.yml` is automatically loaded when `spring.profiles.active=docker` is set. Its values **override** `application.yml` — only the keys that differ need to be specified.

```yaml
# application.yml — local development defaults
spring:
  kafka:
    bootstrap-servers: localhost:9092,localhost:9094,localhost:9096
    properties:
      schema.registry.url: http://localhost:8081
```

```yaml
# application-docker.yml — overrides for Docker network
spring:
  kafka:
    bootstrap-servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
    properties:
      schema.registry.url: http://schema-registry:8081
```

On your local machine, Kafka is reachable at `localhost:9092`. But inside Docker, the producer container and Kafka containers are on the same bridge network, so Kafka is reachable at `kafka-1:9092` (the service name). The Docker profile override cleanly handles this difference without needing environment variable interpolation in the YAML.

The profile is activated in the Dockerfile `ENTRYPOINT`:

```dockerfile
ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "app.jar"]
```

### 1.5 Dockerfile — Multi-Stage Build

```dockerfile
# Stage 1: Build
FROM maven:3.9.6-amazoncorretto-21 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -DskipTests -q

# Stage 2: Runtime
FROM amazoncorretto:21-alpine
WORKDIR /app
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
COPY --from=builder /build/target/aerostream-producer-*.jar app.jar
USER appuser
EXPOSE 8090
ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "app.jar"]
```

**Why multi-stage?**

A naive single-stage build would include Maven, all Maven plugins, all source code, and the full JDK in the final image. That easily produces a 600 MB+ image. Multi-stage solves this:

- **Stage 1** (`builder`) uses the full `maven:3.9.6-amazoncorretto-21` image (~400 MB). It compiles and packages the fat JAR. This stage is never shipped.
- **Stage 2** (runtime) uses `amazoncorretto:21-alpine` (~180 MB), copies only the compiled JAR from Stage 1, and runs it. The final image has no Maven, no source code, no build tools — just the JRE and the JAR.

**Dependency caching trick:**

```dockerfile
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
```

`COPY pom.xml` and `RUN mvn dependency:go-offline` happen before `COPY src`. Docker caches each layer. If only source files change (not `pom.xml`), Docker reuses the cached dependency layer, skipping the 2–5 minute dependency download on every rebuild. This is a standard Dockerfile optimisation for Maven projects.

**Non-root user:**

```dockerfile
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
```

Running the JVM as a non-root user is a security best practice. `-S` creates system accounts (no home directory, no password). The `COPY --chown` ensures the JAR is owned by `appuser`. If the container is ever compromised, the attacker has no root privileges.

### 1.6 `docker-compose.yml` Producer Service

```yaml
producer:
  build:
    context: ./producer
    dockerfile: Dockerfile
  container_name: aerostream-producer
  ports:
    - "${PRODUCER_PORT:-8090}:8090"
  environment:
    SPRING_PROFILES_ACTIVE: docker
    SIMULATOR_EVENTS_PER_SECOND: ${SIMULATOR_EVENTS_PER_SECOND:-500}
  depends_on:
    kafka-1: { condition: service_healthy }
    kafka-2: { condition: service_healthy }
    kafka-3: { condition: service_healthy }
    schema-registry: { condition: service_healthy }
  networks: [aerostream-network]
  restart: unless-stopped
```

The `depends_on` entries use `condition: service_healthy` (not just `condition: service_started`). This means Docker Compose will not start the producer until all three Kafka brokers AND Schema Registry have passed their health checks. This prevents the Spring application from failing on startup with `Connection refused` because Kafka is still initialising.

`SPRING_PROFILES_ACTIVE: docker` is set as an environment variable here as well as in the Dockerfile `ENTRYPOINT`. The environment variable takes precedence, making it easy to override the profile without rebuilding the image.

---

## PROD-2 — Avro Schema: TelemetryEvent

**Priority:** Critical  
**Estimate:** 2 hours  
**Purpose:** Define the data contract for the entire AeroStream pipeline. Everything downstream (Phase 3 Kafka Streams enrichment, Phase 5 ML inference) reads `TelemetryEvent` messages. The schema is defined once here, registered in Schema Registry, and all producers and consumers must be compatible with it.

### 2.1 Why Avro Instead of JSON?

This is a question that comes up constantly in interviews. The table below explains the trade-offs:

| Concern | JSON | Avro |
|---------|------|------|
| Encoding size | Large — field names repeated in every message | ~70% smaller — field names stored once in schema, messages contain only values |
| Schema enforcement | None — any field can appear in any message | Strict — Schema Registry rejects schemas that break backward compatibility |
| Code generation | Manual deserialization | Auto-generates Java POJOs from `.avsc` at build time |
| Schema evolution | No contract — downstream consumers break silently | Controlled — BACKWARD/FULL/NONE compatibility enforced |
| Read speed | Slow — JSON parsing is CPU-intensive | Fast — binary encoded, schema-guided deserialization |

At 10,000 events/sec × 26 fields × ~150 bytes/JSON message = ~1.5 MB/sec per topic. With Avro that drops to ~450 KB/sec — a 3x reduction before LZ4 compression is even applied.

### 2.2 `TelemetryEvent.avsc` — The Schema File

The schema lives at `producer/src/main/avro/TelemetryEvent.avsc`. The `.avsc` extension stands for **Avro Schema**. It is a JSON document that defines the structure of a record type.

```json
{
  "type": "record",
  "name": "TelemetryEvent",
  "namespace": "com.aerostream.avro",
  "doc": "Real-time telemetry event from an F1 car sensor array",
  "fields": [ ... ]
}
```

**Key schema concepts:**

- `"type": "record"` — Avro's equivalent of a Java class or database table row.
- `"namespace"` — maps to the Java package. The generated class will be at `com.aerostream.avro.TelemetryEvent`.
- `"doc"` — documentation string embedded in the schema and visible in Schema Registry UI.

**The 26 fields, grouped by purpose:**

```
Identity (3 fields):
  car_id       string  — "CAR_01" through "CAR_20"
  driver_id    string  — "DRV_01" through "DRV_20"
  session_id   string  — "RACE_2024_MONZA_R1"

Time & Race Progress (3 fields):
  timestamp_ms  long    — Unix epoch milliseconds (when the sensor reading was taken)
  lap           int     — Current lap (1-indexed)
  sector        int     — Track sector 1, 2, or 3

Drivetrain (5 fields):
  speed_kph    double   — Speed in km/h (0 to 335)
  rpm          int      — Engine revolutions per minute (800 to 18,000)
  gear         int      — Current gear (1 to 8)
  throttle_pct double   — Throttle pedal position 0.0–100.0%
  brake_pct    double   — Brake pedal position 0.0–100.0%

Aerodynamics (1 field):
  drs_active   boolean  — Drag Reduction System wing open/closed

Tires (9 fields):
  tire_compound  enum(TireCompound)   — SOFT, MEDIUM, HARD, INTER, WET
  tire_temp_fl/fr/rl/rr  double x4   — Surface temperature per corner (°C)
  tire_wear_fl/fr/rl/rr  double x4   — Wear percentage per corner (0–100%)

Vehicle Systems (3 fields):
  fuel_load_kg    double  — Remaining fuel in kg (starts at 110 kg)
  engine_temp_c   double  — Coolant temperature (°C)
  ers_deploy_pct  double  — ERS energy deployment 0–100%

Dynamics (2 fields):
  g_force_lat  double  — Lateral g-force (cornering, ±5g typical)
  g_force_lon  double  — Longitudinal g-force (accel/braking, ±5g typical)
```

**Why 26 fields and not fewer?** Phase 4 ML needs rich features to predict pit stop timing. Tire wear across all four corners is the primary predictor of when a pit stop is needed. ERS deployment and g-forces are secondary features that improve lap time prediction. Defining all fields now, at the schema level, means Phase 4 never needs a schema migration — all data is available from day one.

**The `TireCompound` Enum:**

```json
{"name": "tire_compound", "type": {
  "type": "enum",
  "name": "TireCompound",
  "namespace": "com.aerostream.avro",
  "symbols": ["SOFT", "MEDIUM", "HARD", "INTER", "WET"]
}}
```

Defining the tire compound as an Avro enum (not a string) means:
1. Schema Registry will reject any message with an invalid compound name (e.g. `"SUPER_SOFT"`) — it will never reach the topic.
2. The Maven plugin generates `TireCompound.java` as a proper Java enum with type safety.
3. Avro encodes enum values as their integer index, not the string, saving bytes.

### 2.3 Schema Registry Integration

When `KafkaAvroSerializer` publishes the first `TelemetryEvent`, it:
1. Serializes the Avro schema to JSON.
2. `POST`s it to `http://schema-registry:8081/subjects/raw-telemetry-value/versions`.
3. Schema Registry validates it against global BACKWARD compatibility (set in Phase 1 by `configure-schema-registry.sh`).
4. If valid, Schema Registry returns a `schema_id` (an integer, e.g. `1`).
5. The serializer prepends a **5-byte header** to every message: `0x00` (magic byte) + 4-byte big-endian schema ID.
6. Consumers use this ID to fetch the correct schema from Schema Registry and deserialize the bytes.

This means consumers never need the `.avsc` file — they get the schema at runtime from Schema Registry.

**Subject naming convention:** `<topic-name>-value`. For `raw-telemetry` the subject is `raw-telemetry-value`. The `-key` variant would be used if the key were also Avro-serialized (ours is a plain `String`).

### 2.4 `SchemaRegistryConfig.java`

```java
@Configuration
public class SchemaRegistryConfig {
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }
}
```

`CachedSchemaRegistryClient` is a Confluent-provided client that caches schema lookups in memory (up to 100 schemas by the `capacity` parameter). Without caching, every Kafka message deserialized would trigger an HTTP call to Schema Registry — catastrophic at 10k events/sec. The cache maps `schema_id → Schema` so repeated schema lookups for the same topic hit memory only.

### 2.5 `SchemaRegistrationTest.java`

```java
@Test
void telemetryEventSchemaIsValid() {
    Schema schema = TelemetryEvent.getClassSchema();
    assertThat(schema.getFields()).hasSize(26);
}
```

This test does not require a running Schema Registry (no `@SpringBootTest`). It reads the schema from the compiled `TelemetryEvent.class` (generated by the avro-maven-plugin). The test verifies:
- Field count is exactly 26 (catch accidental additions or removals)
- All required fields exist by name (`car_id`, `tire_wear_fl`, `g_force_lat`, etc.)
- The `tire_compound` enum contains exactly the 5 expected symbols

Running this test: `mvn test -f producer/pom.xml -Dtest=SchemaRegistrationTest`

### 2.6 `.gitignore` for Generated Sources

```gitignore
target/generated-sources/
```

The `TelemetryEvent.java` and `TireCompound.java` generated by the Maven plugin must **not** be committed. They are regenerated on every `mvn generate-sources`. Committing them causes merge conflicts when the schema changes and wastes repository space with machine-generated code.

---

## PROD-3 — Kafka Producer Configuration

**Priority:** Critical  
**Estimate:** 2 hours  
**Purpose:** Configure the Kafka producer with production-grade reliability and throughput settings. This is the most interview-critical issue of Phase 2 — the settings chosen here directly determine message delivery guarantees, throughput capacity, and partition ordering.

### 3.1 `KafkaProducerConfig.java` — ProducerFactory

The `ProducerFactory` is the Spring abstraction over the Kafka `KafkaProducer`. The `KafkaTemplate` is a high-level send wrapper over it. The configuration is expressed as a `Map<String, Object>` of Kafka producer properties.

**Connection settings:**

```java
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put("schema.registry.url", schemaRegistryUrl);
```

`bootstrapServers` comes from `application.yml` via `@Value`. In Docker it resolves to `kafka-1:9092,kafka-2:9092,kafka-3:9092`. The bootstrap servers are only used for the initial connection — Kafka returns the full cluster metadata after the first successful connection, so listing all three is a resilience measure, not a strict requirement.

**Serialization:**

```java
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
```

The key is a plain Java `String` (the `car_id`). The value is a Confluent `KafkaAvroSerializer` which automatically handles Schema Registry registration and the 5-byte magic prefix.

### 3.2 Idempotent Producer — The Core Guarantee

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
```

This is the most important group of settings. Here is what each does and why all four are needed together:

**`ENABLE_IDEMPOTENCE_CONFIG = true`**

Without idempotence, if the producer sends a batch and the broker receives it but crashes before sending the acknowledgement, the producer will retry. The broker (or its leader-elected replacement) receives the batch a second time and appends it again — **a duplicate**. At 10,000 events/sec, even a 0.01% duplicate rate is 1 duplicate per second.

With idempotence enabled, Kafka assigns each producer a **PID (Producer ID)** and a **sequence number** to each batch. If a duplicate batch arrives at the broker, the broker detects the sequence number has already been committed and silently discards it. Exactly-once at the producer side.

**`ACKS_CONFIG = "all"`**

Determines when the broker considers a write "committed":
- `acks=0` — fire and forget. No guarantee the message was received.
- `acks=1` — leader broker acknowledges once it has written to its local log. If the leader crashes before replication, the message is lost.
- `acks=all` (or `acks=-1`) — the leader waits until all **in-sync replicas** (ISR) have written the message before acknowledging. With `min.insync.replicas=2` (set in Phase 1), the message survives the loss of one broker.

**`RETRIES_CONFIG = Integer.MAX_VALUE`**

If a send fails with a retriable error (network timeout, leader not available during election), the producer will keep retrying. `Integer.MAX_VALUE` effectively means "retry indefinitely". Combined with `delivery.timeout.ms` (default 2 minutes), the producer will retry for up to 2 minutes before giving up on a batch.

**`MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5`**

Controls how many unacknowledged batches can be in flight to a single broker simultaneously. With idempotence enabled, Kafka allows up to 5. Why not 1? Because waiting for each batch to be fully acknowledged before sending the next one would massively reduce throughput. With 5 in-flight requests, the producer pipeline is always busy.

**Note:** Without idempotence, `MAX_IN_FLIGHT > 1` could cause message reordering on retry (batch 2 might succeed while batch 1 is retrying). With idempotence, the broker's sequence number tracking prevents any reordering.

### 3.3 Throughput Tuning

```java
props.put(ProducerConfig.BATCH_SIZE_CONFIG,    65536);     // 64 KB
props.put(ProducerConfig.LINGER_MS_CONFIG,     5);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432L); // 32 MB
```

**`BATCH_SIZE_CONFIG = 65536` (64 KB)**

Kafka batches multiple records into a single network request to reduce overhead. The `batch.size` controls the maximum bytes per batch. The default is 16 KB. 64 KB means:
- Fewer network round trips (each round trip sends 4x more records)
- Better compression ratio (more data = better LZ4 compression)
- Slightly higher latency before the first record in a batch is sent (mitigated by `linger.ms`)

**`LINGER_MS_CONFIG = 5` (5 milliseconds)**

By default, a batch is sent immediately when it has at least one record. With `linger.ms=5`, the producer waits up to 5ms before sending, giving time for more records to accumulate in the batch. At 10k events/sec, a 5ms window accumulates ~50 records. Combined with the 64 KB batch size, this dramatically improves throughput at the cost of ~5ms additional latency — entirely acceptable for telemetry data.

**`COMPRESSION_TYPE_CONFIG = "lz4"`**

LZ4 is a lossless compression algorithm optimised for speed over compression ratio. For Kafka:
- Compression is applied per batch (not per record), so larger batches compress better.
- LZ4 typically achieves ~60% reduction on JSON-like structured data. On Avro binary data, the reduction is smaller (~30-40%) because binary is already compact, but still significant.
- LZ4 is CPU-cheap to compress and decompress, unlike GZIP which is CPU-intensive.
- The compressed bytes travel over the network. Decompression happens at the consumer side.

**`BUFFER_MEMORY_CONFIG = 33554432` (32 MB)**

This is the total memory the producer allocates for buffering unsent records. If the buffer fills up (producer is producing faster than Kafka can consume), the `send()` call will block for up to `max.block.ms` (default 60s), then throw `TimeoutException`. 32 MB gives a generous buffer before backpressure kicks in.

### 3.4 `TelemetryPartitioner.java` — Car-Based Partitioning

```java
public class TelemetryPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 0) return 0;
        if (key == null) return ThreadLocalRandom.current().nextInt(numPartitions);

        return Math.abs(key.hashCode()) % numPartitions;
    }
}
```

**Why a custom partitioner at all?**

Kafka's default partitioner (since Kafka 2.4) uses a "sticky" strategy: it fills a batch with records for any key, sends them together, then picks a new random partition for the next batch. This is good for throughput but provides **no ordering guarantee across records with different keys**.

For Phase 3 Kafka Streams, we need to run **windowed aggregations per car** — e.g., "average tire wear for CAR_01 in the last 10 seconds." Kafka Streams requires that all records with the same key (car_id) arrive on the same partition for this to work correctly. If CAR_01 events are spread across multiple partitions, a Kafka Streams processor on partition 0 sees only a fraction of CAR_01's events, producing wrong aggregations.

**Why `Math.abs(key.hashCode()) % numPartitions`?**

`String.hashCode()` is defined in the Java Language Specification as a deterministic polynomial hash function of the string's characters. It is **stable across all JVM restarts** for the same string value. This means:
- `"CAR_01".hashCode()` returns the same integer every time, on every JVM.
- The same car always maps to the same partition.
- If you restart the producer, cars resume on the same partitions as before — no state disruption for Phase 3 consumers.

`Math.abs()` handles the case where `hashCode()` returns `Integer.MIN_VALUE` (which is negative and has no positive equivalent as a Java `int`), though this is extremely rare for short strings.

**The 20-partition design alignment:**

`raw-telemetry` was created in Phase 1 with 20 partitions — exactly matching the 20 simulated cars. With `TelemetryPartitioner`, each car gets its own partition (in the ideal case), meaning each Kafka Streams task processes exactly one car's stream. This is the optimal co-partitioning for Phase 3.

### 3.5 `TelemetryPartitionerTest.java`

```java
@Test
void sameCarIdAlwaysMapsToSamePartition() {
    // 100 calls with "CAR_01", all must return the same partition
}

@Test
void allTwentyCarsHaveStablePartitions() {
    // 20 cars × 10 calls each — all must be stable
}

@Test
void partitionIsWithinBounds() {
    // All 20 cars must map to partitions 0-19
}
```

These tests use Mockito to mock `Cluster.partitionsForTopic("raw-telemetry")` to return a list of 20 `PartitionInfo` objects. This avoids needing a real Kafka broker in tests. The tests validate:
- **Stability** — same car always gets same partition across repeated calls.
- **Bounds** — no car maps outside the valid partition range.
- **Null key** — falls back to random partition (no NPE).

---

## PROD-4 — Telemetry Simulation Engine

**Priority:** High  
**Estimate:** 3 hours  
**Purpose:** Produce realistic, production-representative telemetry data. Rather than generating random numbers, the simulation models actual F1 physics: speed variations through corners, tire thermal dynamics, fuel consumption, and DRS activation rules. This matters because Phase 5 ML needs data that follows real-world distributions to train a meaningful pit stop predictor.

### 4.1 `CarState.java` — Mutable Per-Car State

```java
@Data
@Builder
public class CarState {
    // Identity (immutable once created)
    private final String carId;
    private final String driverId;
    private final String sessionId;

    // Physics state (mutated by PhysicsEngine.tick() each cycle)
    private double speedKph;
    private int rpm;
    private int gear;
    ...
    // Internal simulation state — never published to Kafka
    private double trackPosition;   // 0.0 to 1.0 around circuit
    private boolean onBrakingZone;
}
```

`CarState` uses Lombok's `@Data` (generates getters, setters, `equals`, `hashCode`, `toString`) and `@Builder` (generates a fluent builder). 20 `CarState` instances are created at simulator start and mutated in-place by `PhysicsEngine.tick()` every simulation cycle.

`trackPosition` (0.0 to 1.0, wrapping) and `onBrakingZone` are **internal simulation state** — they control the physics but are not published to Kafka. Only the sensor readings (speed, temperatures, wear, etc.) become `TelemetryEvent` fields.

### 4.2 `PhysicsEngine.java` — The Tick Loop

```java
public void tick(CarState car) {
    advanceTrackPosition(car);
    updateSpeed(car);
    updateRpm(car);
    updateTireTemperatures(car);
    updateTireWear(car);
    updateFuel(car);
    updateEngineTemp(car);
    updateGForces(car);
    updateDrs(car);
    updateErs(car);
    advanceLapSector(car);
}
```

Each call to `tick()` advances the car's state by one **virtual tick** (50ms of race time, `TICK_SECONDS = 0.05`). The methods execute in a deliberate order because later methods depend on the outputs of earlier ones (e.g., `updateDrs` depends on speed set by `updateSpeed`).

**Track Position:**

```java
private void advanceTrackPosition(CarState car) {
    double distancePerTick = car.getSpeedKph() / 3600.0 * TICK_SECONDS;
    car.setTrackPosition((car.getTrackPosition() + distancePerTick / CIRCUIT_LENGTH_KM) % 1.0);
}
```

`speedKph / 3600.0` converts km/h to km/s. Multiplied by 0.05s gives distance in km covered this tick. Divided by the circuit length (5 km) gives fractional progress around the track. Modulo 1.0 wraps at the start/finish line.

**Braking Zones:**

```java
private boolean isInBrakingZone(double pos) {
    return (pos > 0.18 && pos < 0.22)
        || (pos > 0.43 && pos < 0.47)
        || (pos > 0.68 && pos < 0.72)
        || (pos > 0.83 && pos < 0.87);
}
```

Four braking zones are defined at fractional track positions. At a 5 km circuit, these correspond to ~0.9 km, ~2.15 km, ~3.4 km, and ~4.15 km — roughly where heavy braking corners would be on a typical F1 circuit. When the car enters a braking zone:
- Speed decreases by 15 kph/tick (hard braking) + Gaussian noise (±2 kph for variation).
- Throttle drops to 0%, brakes engage at 80–100%.
- Outside braking zones: speed increases by 8 kph/tick (acceleration) + Gaussian noise, throttle at 90–100%.
- Max speed is capped at 335 kph (modern F1 top speed).

**Tire Temperature Model:**

```java
private void updateTireTemperatures(CarState car) {
    double speedFactor  = car.getSpeedKph() / 300.0;
    double heatInput    = speedFactor * TIRE_HEAT_RATE + random.nextGaussian() * 0.1;
    double coolInput    = TIRE_COOL_RATE * (car.getTireTempFl() - OPTIMAL_TIRE_TEMP) / OPTIMAL_TIRE_TEMP;
    double delta        = heatInput - coolInput;
    // ...
}
```

This is a simplified thermal equilibrium model:
- **Heat input** is proportional to speed (higher speed = more friction = more heat). Gaussian noise simulates road surface variation and aerodynamic effects.
- **Cool input** is proportional to the temperature *excess* above the optimal working temperature (95°C). This is a negative feedback loop — the further above optimal, the stronger the cooling effect. At exactly 95°C, `coolInput = TIRE_COOL_RATE * 0/95 = 0`.
- The net `delta` is added to each corner's temperature. Front-right is 5% hotter (`delta * 1.05`) because under braking, weight transfers to the front-right corner. Rear tires run cooler (`delta * 0.85-0.95`).

At simulation start, all tires begin at 80°C (below the working window). Over the first 1-2 simulated laps, temperatures climb to 90–115°C — this is realistic "tyre warm-up" behaviour.

**Tire Wear Model:**

```java
double compoundMultiplier = switch (car.getTireCompound()) {
    case SOFT  -> 1.4;
    case HARD  -> 0.7;
    case INTER -> 0.9;
    case WET   -> 0.6;
    default    -> 1.0;  // MEDIUM
};
double wearRate = TIRE_WEAR_PER_TICK * (car.getSpeedKph() / 200.0) * compoundMultiplier;
```

Wear rate is speed-dependent (faster = more wear) and compound-dependent. Soft tires wear 40% faster than Medium (the `1.4` multiplier). Hard tires wear 30% slower (the `0.7` multiplier). Wet and Inter compounds are designed for wet surfaces and have different wear profiles. This directly mirrors how F1 strategy works: soft tires are faster but need earlier pit stops.

**Fuel Burn:**

```java
private static final double FUEL_BURN_PER_TICK = 0.000028; // ~2 kg/lap at 70 ticks/lap
car.setFuelLoadKg(Math.max(0, car.getFuelLoadKg() - FUEL_BURN_PER_TICK));
```

A real F1 car burns approximately 2 kg of fuel per lap at race pace. At 50ms ticks, a lap takes roughly 70 ticks (70 × 0.05s = 3.5s — accelerated simulation time). `0.000028 kg/tick × 70 ticks = 0.00196 kg/lap ≈ 2 kg/lap`. Starting fuel load is 110 kg, giving roughly 55 simulated laps before the tank empties.

**DRS Activation:**

```java
private void updateDrs(CarState car) {
    car.setDrsActive(car.getSpeedKph() > 280 && !car.isOnBrakingZone());
}
```

DRS (Drag Reduction System) opens the rear wing's moveable flap to reduce aerodynamic drag, enabling higher top speeds. In real F1, it can only be used in designated DRS zones at speeds above a threshold. Our simplified model: DRS activates whenever the car is above 280 kph and not in a braking zone. This produces realistic boolean patterns — DRS is active on straights, closed under braking.

**ERS Deployment:**

```java
double target = car.isOnBrakingZone() ? 0 : 60 + random.nextDouble() * 40;
car.setErsDeployPct(clamp(car.getErsDeployPct() + (target - car.getErsDeployPct()) * 0.1, 0, 100));
```

ERS (Energy Recovery System) harvests energy under braking and deploys it under acceleration. The deployment percentage transitions smoothly using a first-order lag filter (multiply by 0.1): when the target changes, the actual value approaches it gradually rather than jumping instantly. This produces natural-looking deployment curves.

### 4.3 `TelemetryEventMapper.java` — CarState to Avro

```java
public static TelemetryEvent toAvro(CarState car) {
    return TelemetryEvent.newBuilder()
        .setCarId(car.getCarId())
        .setTimestampMs(System.currentTimeMillis())
        ...
        .build();
}
```

The mapper is a pure static function with no Spring dependencies — a `final` utility class. It converts the mutable `CarState` (a simulation object) into an immutable `TelemetryEvent` Avro record (a data transfer object for Kafka). `System.currentTimeMillis()` stamps the wall-clock time at the moment of mapping, so `timestamp_ms` reflects when the telemetry was generated, not when the tick was scheduled.

All doubles are rounded to 2 decimal places (`Math.round(value * 100.0) / 100.0`) to reduce noise and keep Avro binary representations compact.

### 4.4 `TelemetrySimulator.java` — The Scheduler

```java
public synchronized void start() {
    if (running.compareAndSet(false, true)) {
        initCars();
        activeCarsGauge.set(cars.size());
        long tickIntervalMicros = Math.max(1, 1_000_000L / Math.max(1, props.getEventsPerSecond() / cars.size()));
        executor = Executors.newScheduledThreadPool(4);
        executor.scheduleAtFixedRate(this::tickAll, 0, tickIntervalMicros, TimeUnit.MICROSECONDS);
    }
}
```

**Tick interval calculation:**

`eventsPerSecond / numCars` gives "ticks per second per car". Inverting that gives "microseconds between ticks". At 10,000 events/sec with 20 cars: `10000 / 20 = 500 ticks/sec per car`. `1,000,000 / 500 = 2000 µs = 2ms` between ticks. `scheduleAtFixedRate` with `TimeUnit.MICROSECONDS` gives microsecond-precision scheduling.

**`compareAndSet(false, true)` — Thread Safety:**

`AtomicBoolean.compareAndSet` atomically checks if the value is `false` and sets it to `true` only if it was `false`. This is a lock-free check-and-set operation that prevents two simultaneous `start()` calls from creating two executor services and running the simulator twice.

**`synchronized` on `start()` and `stop()`:** The method-level `synchronized` ensures that if `stop()` is called while `start()` is partway through initialization, it cannot run concurrently and observe a partially-initialized state.

**`scheduleAtFixedRate` vs `scheduleWithFixedDelay`:**

- `scheduleAtFixedRate` fires every N microseconds **from the start time** — if a tick takes 1ms and the interval is 2ms, the next tick fires 1ms later.
- `scheduleWithFixedDelay` fires N microseconds **after the previous run completes** — if a tick takes 1ms and the delay is 2ms, the next tick fires 3ms later.

`scheduleAtFixedRate` is correct here because we want a constant event production rate, not constant time between runs.

**Pool size = 4:**

`Executors.newScheduledThreadPool(4)` creates 4 daemon threads. `tickAll()` iterates over 20 cars sequentially in a single thread. The additional threads in the pool serve as fallback schedulers — if one thread is delayed, another can pick up the next scheduled tick without falling behind.

**Staggered start positions:**

```java
.trackPosition(i / (double) props.getNumCars())
```

Car 1 starts at position 0.05 (1/20 of the track), Car 2 at 0.10, ..., Car 20 at 1.0 (which wraps to 0.0). This spreads the 20 cars evenly around the circuit, preventing all 20 from hitting the same braking zone simultaneously and producing unrealistic synchronised speed drops across all cars.

### 4.5 `SimulatorController.java` — REST Lifecycle API

```java
@PostMapping("/start")     → simulator.start()
@PostMapping("/stop")      → simulator.stop()
@GetMapping("/status")     → SimulatorStatus DTO
```

The `SimulatorStatus` JSON response looks like:

```json
{
  "running": true,
  "activeCarCount": 20,
  "totalPublished": 152847,
  "totalDlqRouted": 0,
  "eventsPerSecond": 500
}
```

---

## PROD-5 — Dead Letter Queue (DLQ)

**Priority:** High  
**Estimate:** 2 hours  
**Purpose:** Ensure that when a Kafka producer publish fails, the failed message is not silently dropped. It is captured in the `dlq-telemetry` topic with full diagnostic context — which car, which topic, when, and what went wrong. The DLQ is also the proof-of-concept for the pipeline's error handling — if it works for the producer, the pattern extends to Phase 3 and Phase 5.

### 5.1 Why a Dead Letter Queue?

Without a DLQ, what happens when a publish fails?

```java
kafkaTemplate.send(rawTelemetryTopic, car.getCarId(), event)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("Publish failed for {}: {}", car.getCarId(), ex.getMessage());
            // Message is gone — no retry, no record, no visibility
        }
    });
```

That error log line is the only trace. No way to replay the failed message, no way to alert on DLQ rate, no way to understand whether failures are transient or systematic. In production, silent data loss compounds — a producer publishing 10k events/sec losing even 0.01% is 1 lost event per second.

A DLQ solves this by publishing failed records to a separate topic with diagnostic headers. Operations teams can:
- Monitor DLQ rate in Prometheus/Grafana to detect systematic failures.
- Replay DLQ messages once the root cause is fixed.
- Use headers to understand which original topic, which car, and what exception caused the failure.

### 5.2 `DlqPublisher.java` — A Separate Producer for Errors

```java
@Component
@Slf4j
public class DlqPublisher {

    private final KafkaTemplate<String, byte[]> dlqTemplate;

    public DlqPublisher(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                        MeterRegistry meterRegistry,
                        @Lazy TelemetrySimulator simulator) {
        Map<String, Object> props = new HashMap<>();
        // ...
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        this.dlqTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        simulator.setDlqPublisher(this);
    }
```

**Deliberate design choices:**

- **`ByteArraySerializer` (not `KafkaAvroSerializer`)** — The DLQ receives raw bytes. If the original failure was a serialization error, we cannot re-attempt Avro serialization. Storing a plain byte representation of the failed record is safer and always works.
- **`ENABLE_IDEMPOTENCE = false`** — The DLQ producer does not need idempotence guarantees. DLQ records are already "error records" — a duplicate in the DLQ is far less harmful than a duplicate in `raw-telemetry`.
- **`ACKS = "1"`** — Only the leader broker acknowledges. This is intentionally weaker than the main producer's `acks=all`. The DLQ is best-effort — if even the DLQ write fails, we log it and move on. We cannot get into an infinite error loop of "DLQ write failed, write that failure to DLQ..."
- **`RETRIES = 0`** — No retries on DLQ writes. Non-retriable by design.

**Why `@Lazy TelemetrySimulator`?**

`TelemetrySimulator` depends on `MeterRegistry` and `KafkaTemplate`. `DlqPublisher` depends on `TelemetrySimulator`. If both were eager beans, Spring would encounter a **circular dependency** at startup: `TelemetrySimulator` can't be created without `DlqPublisher`, but `DlqPublisher` can't be created without `TelemetrySimulator`.

`@Lazy` tells Spring: "Don't inject `TelemetrySimulator` during `DlqPublisher`'s construction. Inject a proxy. The actual `TelemetrySimulator` bean will be resolved the first time the proxy's method is called." This breaks the circular dependency. The call to `simulator.setDlqPublisher(this)` — which happens in `DlqPublisher`'s constructor — actually calls through the lazy proxy to the real `TelemetrySimulator` instance, wiring them together.

**The 5 diagnostic headers:**

```java
dlqRecord.headers()
    .add("X-Error-Message",   truncate(cause.getMessage()))
    .add("X-Original-Topic",  originalTopic.getBytes())
    .add("X-Error-Timestamp", String.valueOf(Instant.now().toEpochMilli()).getBytes())
    .add("X-Exception-Class", cause.getClass().getName().getBytes())
    .add("X-Original-Car-Id", carId.getBytes());
```

Kafka record headers are key-value byte pairs attached to the record, separate from the value payload. They are visible in Kafka UI without deserializing the value. Each header serves a specific operational purpose:

| Header | Purpose |
|--------|---------|
| `X-Error-Message` | Human-readable failure reason (truncated to 500 bytes) |
| `X-Original-Topic` | Which topic the message was intended for (`raw-telemetry`) |
| `X-Error-Timestamp` | Exact epoch millisecond the failure was detected |
| `X-Exception-Class` | Java exception class name (e.g. `SerializationException`) |
| `X-Original-Car-Id` | Which car produced the failed record |

### 5.3 `PoisonPillGenerator.java` — Testing the DLQ Path

```java
private TelemetryEvent createPoisonPill(int index) {
    return TelemetryEvent.newBuilder()
        .setTimestampMs(-1L)        // invalid negative timestamp
        .setLap(-999)               // impossible lap number
        .setSpeedKph(9999.99)       // impossible speed
        .setTireTempFl(500.0)       // impossibly hot tire (max 130°C in physics model)
        .setTireWearFl(999.0)       // over 100% wear
        .setFuelLoadKg(-10.0)       // negative fuel
        .setGForceLat(50.0)         // impossible g-force (max ~6g in real F1)
        ...
        .build();
```

The Avro schema does not enforce value ranges — it enforces **types** (int, double, string) but not business logic constraints (e.g. "speed must be 0–400 kph"). Poison pills are valid Avro objects but semantically invalid for a real F1 car. In Phase 3/5, consumers can add validation that detects these and routes them to DLQ again at the consumer side.

**Why do we need this?** To prove the DLQ path works end-to-end during development. Without a deliberate way to trigger DLQ-bound messages, you cannot verify the acceptance criterion "dlq-telemetry topic has 5 messages with X-Error-Message header."

### 5.4 `TestController.java` — The Trigger Endpoint

```java
@RestController
@RequestMapping("/api/test")
@Profile("!production")
public class TestController {

    @PostMapping("/poison-pill")
    public ResponseEntity<Map<String, Object>> poisonPill(
            @RequestParam(defaultValue = "5") int count) {
        int sent = generator.generateAndSend(count);
        return ResponseEntity.ok(Map.of(
            "requested", count, "sent", sent,
            "dlqTopic", "dlq-telemetry", "timestamp", Instant.now().toString()
        ));
    }
}
```

`@Profile("!production")` is Spring's profile exclusion syntax. The `TestController` bean will only be created if the active Spring profile is **not** `production`. In `application.yml`, the default profile is not `production`, so this endpoint is available in development and Docker (which uses the `docker` profile). It is entirely absent from any `production`-profiled deployment.

---

## PROD-6 — Micrometer Metrics & Observability

**Priority:** Medium  
**Estimate:** 1.5 hours  
**Purpose:** Make the producer's internal state visible. Without metrics, you cannot tell whether the simulator is producing 500 events/sec or 5000 events/sec, whether the DLQ is empty or filling up, or whether Kafka publish latency is acceptable. Micrometer + Prometheus + Grafana is the standard Spring Boot observability stack.

### 6.1 What Is Micrometer?

Micrometer is to metrics what SLF4J is to logging — a **vendor-neutral facade**. You write `counter.increment()` in your code. Micrometer translates that to Prometheus format, DataDog format, InfluxDB format, etc. — you switch destinations by changing a dependency, not your code.

With `micrometer-registry-prometheus` on the classpath, Spring Boot Actuator automatically exposes a `/actuator/prometheus` endpoint in Prometheus text format:

```
# HELP telemetry_events_published_total Total telemetry events successfully published to Kafka
# TYPE telemetry_events_published_total counter
telemetry_events_published_total{topic="raw-telemetry",} 152847.0
```

### 6.2 Metrics in `TelemetrySimulator`

**Counter — `telemetry.events.published`:**

```java
this.eventsPublishedCounter = Counter.builder("telemetry.events.published")
    .description("Total telemetry events successfully published to Kafka")
    .tag("topic", "raw-telemetry")
    .register(meterRegistry);
```

A Micrometer `Counter` is a monotonically increasing number — it only goes up. Counters are ideal for "total number of X that happened." In Prometheus, you use `rate()` to get the per-second rate:

```promql
rate(telemetry_events_published_total[1m])
```

This gives events/sec averaged over the last 1 minute — the primary throughput metric.

**Gauge — `telemetry.active.cars`:**

```java
Gauge.builder("telemetry.active.cars", activeCarsGauge, AtomicLong::get)
    .description("Number of cars currently being simulated")
    .register(meterRegistry);
```

A Micrometer `Gauge` reflects a current value that can go up or down — like a thermometer. The `AtomicLong::get` is a function reference telling Micrometer how to read the current value each time Prometheus scrapes. When the simulator stops, `activeCarsGauge.set(0)`, and the gauge immediately reflects 0.

**Counter — `telemetry.events.dlq` (in `DlqPublisher`):**

```java
this.dlqCounter = Counter.builder("telemetry.events.dlq")
    .description("Total events routed to dead letter queue")
    .tag("topic", DLQ_TOPIC)
    .register(meterRegistry);
```

The DLQ counter is incremented in `DlqPublisher.publishToDlq()` on successful DLQ write. In Prometheus:

```promql
rate(telemetry_events_dlq_total[1m])
```

A non-zero DLQ rate is an **alert signal** — it means the main producer is failing for some records.

### 6.3 Prometheus Scrape Job — `prometheus.yml`

```yaml
- job_name: aerostream-producer
  metrics_path: /actuator/prometheus
  static_configs:
    - targets:
        - aerostream-producer:8090
      labels:
        service: producer
        cluster: aerostream-local
```

This was added to `infra/prometheus/prometheus.yml` alongside the existing Kafka broker scrape jobs. The `aerostream-producer:8090` hostname resolves because the producer container is on the same `aerostream-network` Docker bridge as the Prometheus container.

`metrics_path: /actuator/prometheus` overrides the default `/metrics` path — Spring Boot Actuator uses a non-standard path.

After modifying `prometheus.yml`, Prometheus can be reloaded without restart:

```bash
curl -X POST http://localhost:9090/-/reload
```

### 6.4 Grafana Dashboard — `producer-dashboard.json`

The dashboard is a JSON file provisioned automatically by the `infra/grafana/provisioning/dashboards/provider.yml` file system provider set up in Phase 1 (INFRA-5). Any `.json` file placed in `infra/grafana/dashboards/` is loaded automatically on Grafana startup.

The dashboard has 7 panels:

| Panel | Type | PromQL Query | Purpose |
|-------|------|-------------|---------|
| Events Published / sec | Time series | `rate(telemetry_events_published_total[1m])` | Live throughput |
| DLQ Rate / sec | Time series | `rate(telemetry_events_dlq_total[1m])` | Error rate |
| Active Cars | Stat | `telemetry_active_cars` | Simulator running state |
| Total Events Published | Stat | `telemetry_events_published_total` | Cumulative count |
| Total DLQ Events | Stat | `telemetry_events_dlq_total` | Cumulative errors |
| Kafka Producer Record Send Rate | Time series | `rate(kafka_producer_record_send_total[1m])` | Raw Kafka client metric |
| Producer Request Latency (avg) | Time series | `kafka_producer_request_latency_avg` | Broker round-trip time |

The dashboard auto-refreshes every 5 seconds (`"refresh": "5s"`) and defaults to a 15-minute time window (`"time": {"from": "now-15m", "to": "now"}`). The `uid: "aerostream-producer"` means the dashboard URL is stable — `/d/aerostream-producer/aerostream-producer`.

---

## How It All Connects: The Data Flow

```
Simulation Tick (every 2ms at 10k/sec)
              │
              ▼
    PhysicsEngine.tick(CarState)
    ─ advances all 26 sensor values for one car
    ─ braking zones, tire temps, wear, fuel, ERS...
              │
              ▼
    TelemetryEventMapper.toAvro(CarState)
    ─ stamps current timestamp_ms
    ─ rounds doubles to 2dp
    ─ returns immutable TelemetryEvent Avro object
              │
              ▼
    KafkaTemplate.send("raw-telemetry", carId, event)
    ─ TelemetryPartitioner assigns partition: Math.abs(carId.hashCode()) % 20
    ─ KafkaAvroSerializer: schema_id header + Avro binary payload
    ─ Record added to in-memory batch (up to 64 KB)
    ─ Batch flushed after 5ms (linger.ms) or when full
    ─ LZ4 compressed → sent to Kafka broker
    ─ Broker replicates to all 3 replicas (acks=all)
              │
         ┌────┴────┐
         ▼ success  ▼ failure
  eventsPublished  DlqPublisher.sendRaw()
  Counter.incr()   ─ ByteArraySerializer (no Avro)
                   ─ 5 diagnostic headers
                   ─ Written to dlq-telemetry (ACKS=1, RETRIES=0)
                   dlqCounter.incr()
              │
              ▼
    Prometheus scrapes /actuator/prometheus every 15s
    ─ telemetry_events_published_total
    ─ telemetry_events_dlq_total
    ─ telemetry_active_cars
              │
              ▼
    Grafana polls Prometheus
    ─ rate() queries → events/sec panels
    ─ Auto-refresh 5s
```

---

## Phase 2 Startup Sequence

**Prerequisites:** Phase 1 cluster must be running (`docker compose ps` shows all kafka-1/2/3 and schema-registry as `(healthy)`).

```bash
# 1. Build the producer JAR (skipping tests for speed)
mvn clean package -f producer/pom.xml -DskipTests

# 2. Build the Docker image
docker compose build producer

# 3. Start the producer container
docker compose up -d producer

# 4. Wait ~15 seconds for Spring Boot to start, then verify health
curl http://localhost:8090/actuator/health
# Expected: {"status":"UP"}

# 5. Reload Prometheus to pick up the new producer scrape job
curl -X POST http://localhost:9090/-/reload

# 6. Start the simulator
curl -X POST http://localhost:8090/api/simulator/start

# 7. Check simulator status
curl http://localhost:8090/api/simulator/status
# Expected: {"running":true,"activeCarCount":20,...}

# 8. (Optional) Verify Schema Registry has the schema
curl http://localhost:8081/subjects
# Expected: ["raw-telemetry-value"]

# 9. (Optional) Test DLQ
curl -X POST "http://localhost:8090/api/test/poison-pill?count=5"
# Expected: {"requested":5,"sent":5,...}

# 10. Confirm DLQ topic in Kafka UI or via CLI
# Navigate to http://localhost:8080 → Topics → dlq-telemetry → Messages
# Each message should show X-Error-Message header
```

---

## API Reference

| Method | Path | Description | Example Response |
|--------|------|-------------|-----------------|
| `GET` | `/actuator/health` | Spring Boot health check | `{"status":"UP"}` |
| `GET` | `/actuator/prometheus` | Prometheus metrics endpoint | Prometheus text format |
| `POST` | `/api/simulator/start` | Start the simulation engine | `"Simulator started"` |
| `POST` | `/api/simulator/stop` | Stop the simulation engine | `"Simulator stopped"` |
| `GET` | `/api/simulator/status` | Simulator state and counters | `{"running":true,"activeCarCount":20,"totalPublished":152847,...}` |
| `POST` | `/api/test/poison-pill?count=5` | Send N malformed events to DLQ | `{"requested":5,"sent":5,"dlqTopic":"dlq-telemetry",...}` |

---

## Definition of Done Verification

Run these checks in order after Phase 2 startup:

```bash
# 1. Schema registered in Schema Registry
curl -s http://localhost:8081/subjects
# ✓ ["raw-telemetry-value"]

# 2. Producer service healthy
curl -s http://localhost:8090/actuator/health | python3 -m json.tool
# ✓ {"status": "UP"}

# 3. Simulator publishing
curl -X POST http://localhost:8090/api/simulator/start
sleep 5
curl -s http://localhost:8090/api/simulator/status | python3 -m json.tool
# ✓ "running": true, "totalPublished" > 0

# 4. Partitioning visible in Kafka UI
# http://localhost:8080 → Topics → raw-telemetry → Messages
# Filter by key "CAR_01" → all messages on the same partition

# 5. DLQ working
curl -X POST "http://localhost:8090/api/test/poison-pill?count=5"
# http://localhost:8080 → Topics → dlq-telemetry → Messages → check X-Error-Message header

# 6. Prometheus scraping producer
curl -s "http://localhost:9090/api/v1/targets" | python3 -m json.tool | grep aerostream-producer
# ✓ "health": "up"

# 7. Events counter incrementing
curl -s http://localhost:8090/actuator/prometheus | grep telemetry_events_published_total
# ✓ telemetry_events_published_total{topic="raw-telemetry",} <some positive number>
```

---

## Key Design Decisions Summary

| Decision | Choice | Why Not the Alternative |
|----------|--------|------------------------|
| Language | Java 21 | Python is slower for high-throughput producers; Java has native Kafka client and Spring ecosystem |
| Build tool | Maven + avro-maven-plugin | Gradle would also work but Maven has better IDE support for Avro code generation |
| Schema format | Apache Avro | JSON: 3x larger, no schema enforcement. Protobuf: harder Schema Registry integration |
| Delivery guarantee | Idempotent producer (`acks=all`) | `acks=1` risks data loss if leader crashes; `acks=0` is fire-and-forget |
| Batching | 64KB + 5ms linger + LZ4 | Default (16KB, no linger, no compression) cannot sustain 10k events/sec without network saturation |
| Partitioning | Custom hash by `car_id` | Default sticky partitioner breaks per-car ordering required by Phase 3 Kafka Streams |
| DLQ serializer | `ByteArraySerializer` (not Avro) | If the failure was a serialization error, re-attempting Avro serialization would fail again |
| DLQ delivery | `acks=1`, `retries=0` | DLQ must not retry or block; a failed DLQ write is logged and accepted |
| Circular dep (DLQ ↔ Simulator) | `@Lazy` injection | Constructor injection would cause `BeanCurrentlyInCreationException` at startup |
| Test profile guard | `@Profile("!production")` | Hardcoded `if` check is fragile; Spring profiles are declarative and standard |
| Metrics | Micrometer + Prometheus | JMX alone is not scrape-friendly; Micrometer is vendor-neutral and standard for Spring Boot |
