# Spring Boot Essentials — Aerostream Edition

> A focused guide to the Spring concepts you need to understand every part of this codebase.
> Each concept is explained from first principles, then immediately tied back to a real class in this project.

---

## Table of Contents

1. [What is Spring Boot?](#1-what-is-spring-boot)
2. [The Application Context — Spring's Object Manager](#2-the-application-context--springs-object-manager)
3. [Beans and Stereotypes — @Component, @Configuration, @RestController](#3-beans-and-stereotypes)
4. [Dependency Injection — How Beans Find Each Other](#4-dependency-injection)
5. [@SpringBootApplication — The Entry Point](#5-springbootapplication--the-entry-point)
6. [Configuration: @Configuration and @Bean](#6-configuration-and-bean)
7. [Injecting Properties: @Value](#7-injecting-properties-value)
8. [Type-Safe Configuration: @ConfigurationProperties](#8-type-safe-configuration-configurationproperties)
9. [Spring Profiles — Environment-Specific Config](#9-spring-profiles)
10. [application.yml — The Config File](#10-applicationyml--the-config-file)
11. [The Web Layer — @RestController, @GetMapping, @PostMapping](#11-the-web-layer)
12. [Application Lifecycle — @EventListener](#12-application-lifecycle--eventlistener)
13. [Spring Kafka — KafkaTemplate and ProducerFactory](#13-spring-kafka)
14. [Spring Boot Actuator and Micrometer Metrics](#14-spring-boot-actuator-and-micrometer-metrics)
15. [Lombok — The Spring Developer's Shorthand](#15-lombok--the-spring-developers-shorthand)
16. [Circular Dependencies and @Lazy](#16-circular-dependencies-and-lazy)
17. [Spring Boot Starters — How Auto-Configuration Works](#17-spring-boot-starters--how-auto-configuration-works)
18. [Putting It All Together — The Aerostream Bean Graph](#18-putting-it-all-together)

---

## 1. What is Spring Boot?

### Plain Spring vs. Spring Boot

**Spring Framework** is a giant toolkit for building Java applications. It handles dependency injection, web layers, data access, messaging, and much more. The catch: you had to wire everything together yourself — XML files, dozens of config classes, manual bean registration.

**Spring Boot** is an opinionated wrapper around Spring Framework. It has one goal: *eliminate boilerplate*. It does this through three ideas:

| Idea | What it means |
|------|--------------|
| **Auto-configuration** | Spring Boot looks at your classpath and automatically configures beans you'd have written by hand. Add `spring-kafka` to `pom.xml` and you get a working `KafkaTemplate` with zero extra code — unless you want to customize it. |
| **Starters** | Curated dependency bundles. `spring-boot-starter-web` pulls in Spring MVC, Tomcat, Jackson, and more — all at compatible versions. |
| **Opinionated defaults** | Sensible out-of-the-box settings. Override only what you need. |

### The Boot lifecycle in one sentence
When you run `main()`, Spring Boot scans your code, discovers all beans, wires them together, starts an embedded Tomcat server, and hands control back to your application — in a few seconds.

---

## 2. The Application Context — Spring's Object Manager

The **ApplicationContext** is the heart of Spring. Think of it as a smart registry that:

1. Creates objects (called **beans**) on startup.
2. Injects the right dependencies into each bean automatically.
3. Manages their lifecycle (init, ready, destroy).

You never call `new MyService()` in Spring code. You ask the context for a bean and it gives you the right instance. This is called **Inversion of Control (IoC)** — you don't control object creation; Spring does.

```
Your Code            ApplicationContext
   |                       |
   |-- I need a Bean X --> |
   |                       |-- Creates X, injects its deps
   |<-- Here is X -------- |
```

In Aerostream, the context holds beans like `TelemetrySimulator`, `KafkaTemplate`, `SimulatorController`, `MeterRegistry`, and ~10 others — all wired together automatically.

---

## 3. Beans and Stereotypes

A **bean** is any object that Spring manages. You tell Spring about a class by annotating it. These annotations are called **stereotypes**.

### Core stereotype annotations

| Annotation | Meaning | Used in Aerostream |
|-----------|---------|-------------------|
| `@Component` | Generic Spring-managed bean | `TelemetrySimulator`, `PhysicsEngine`, `DlqPublisher`, `PoisonPillGenerator` |
| `@Service` | Semantic alias for `@Component`, signals business logic | Not used directly, but `@Component` fills this role |
| `@Repository` | `@Component` for data access classes | Not used (no database) |
| `@RestController` | `@Component` + marks as HTTP handler | `SimulatorController`, `TestController` |
| `@Configuration` | A class that declares `@Bean` methods | `KafkaProducerConfig`, `SchemaRegistryConfig` |

All four of `@Component`, `@Service`, `@Repository`, `@RestController` ultimately behave the same way at the container level — they all register the class as a bean. The differences are semantic (for readability) and functional only for `@Repository` (which adds exception translation for DB errors) and `@RestController` (which handles HTTP).

### Component Scanning

Spring Boot automatically scans all packages at or below your `main` class package. Because `AerostreamProducerApplication` lives in `com.aerostream`, every class in `com.aerostream.**` annotated with a stereotype is auto-discovered.

```
com.aerostream                      ← @SpringBootApplication here
├── config/
│   ├── KafkaProducerConfig         ← @Configuration (found automatically)
│   └── SchemaRegistryConfig        ← @Configuration (found automatically)
├── kafka/
│   ├── DlqPublisher                ← @Component
│   └── TelemetryPartitioner        ← NOT a bean (no annotation)
├── simulation/
│   ├── TelemetrySimulator          ← @Component
│   ├── SimulatorController         ← @RestController
│   ├── PhysicsEngine               ← @Component
│   └── SimulatorProperties         ← not scanned, registered via @EnableConfigurationProperties
└── testing/
    ├── TestController              ← @RestController (conditional on profile)
    └── PoisonPillGenerator         ← @Component
```

---

## 4. Dependency Injection

Once beans are registered, Spring wires them together. This is **Dependency Injection (DI)**: instead of a class creating its own dependencies, it declares what it needs and Spring provides them.

### Three styles of injection

**1. Constructor Injection** *(preferred, used throughout Aerostream)*

```java
// Spring sees: "TelemetrySimulator needs PhysicsEngine, KafkaTemplate, ..."
// It creates those beans first, then passes them here.
public TelemetrySimulator(
    PhysicsEngine physicsEngine,
    KafkaTemplate<String, TelemetryEvent> kafkaTemplate,
    SimulatorProperties simulatorProperties,
    MeterRegistry meterRegistry
) { ... }
```

- Immutable (fields can be `final`).
- Dependencies are always present — the object cannot exist in a half-wired state.
- Easier to unit test (just call `new MyClass(mockDep1, mockDep2)`).

**2. Field Injection** *(avoid)*

```java
@Autowired  // Spring injects directly into the field via reflection
private PhysicsEngine physicsEngine;
```

This is discouraged because it hides dependencies and makes testing harder. **Aerostream never uses this.**

**3. Setter Injection** *(rarely used)*

```java
@Autowired
public void setPhysicsEngine(PhysicsEngine engine) { ... }
```

### How Spring resolves which bean to inject

When Spring sees a constructor parameter of type `PhysicsEngine`, it looks in the context for a bean of that type. If exactly one exists, it injects it. If zero or more than one exist, it throws an error (you'd need `@Primary` or `@Qualifier` to disambiguate — not needed in Aerostream since each type has exactly one bean).

### @Autowired — when you need it explicitly

With a single constructor, `@Autowired` is optional since Spring 4.3 — Spring infers that the single constructor is the injection point. This is why none of the constructor-injected classes in Aerostream have `@Autowired`.

---

## 5. @SpringBootApplication — The Entry Point

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

`@SpringBootApplication` is a **meta-annotation** — a shortcut for three annotations:

| Contained annotation | What it does |
|---------------------|-------------|
| `@SpringBootConfiguration` | Marks this as the root `@Configuration` class |
| `@EnableAutoConfiguration` | Turns on Spring Boot's auto-configuration magic |
| `@ComponentScan` | Scans `com.aerostream` (and sub-packages) for beans |

`SpringApplication.run(...)` bootstraps the entire application:
1. Creates the `ApplicationContext`.
2. Runs component scan, registers all beans.
3. Fires auto-configuration (creates `KafkaTemplate`, `MeterRegistry`, Tomcat, etc. based on classpath).
4. Starts embedded Tomcat on port 8090.
5. Fires `ApplicationReadyEvent` (triggering `TelemetrySimulator.onApplicationReady()`).

---

## 6. Configuration and @Bean

`@Configuration` marks a class as a **source of bean definitions**. Inside, methods annotated with `@Bean` are called by Spring during startup — the return value becomes a managed bean.

### How it works

```java
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean  // Spring calls this method; the returned object becomes a bean
    public ProducerFactory<String, TelemetryEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // ... more settings ...
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean  // This bean depends on the producerFactory() bean above
    public KafkaTemplate<String, TelemetryEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

When `kafkaTemplate()` calls `producerFactory()`, Spring doesn't actually call your method again — it intercepts the call and returns the already-created `ProducerFactory` singleton from the context. This interception is why `@Configuration` classes must not be `final` (Spring subclasses them using CGLIB proxying).

### @Configuration vs. @Component — key difference

| Aspect | `@Configuration` | `@Component` |
|--------|-----------------|-------------|
| Can define `@Bean` methods | Yes (CGLIB-proxied) | Yes (but not proxied — inter-bean calls will create NEW instances) |
| Primary purpose | Declaring beans | Being a bean itself |

In Aerostream, `KafkaProducerConfig` and `SchemaRegistryConfig` are both `@Configuration` classes that define `@Bean` methods.

---

## 7. Injecting Properties: @Value

`@Value` injects a single value from `application.yml` (or environment variables) directly into a field or constructor parameter.

### Syntax

```java
@Value("${spring.kafka.bootstrap-servers}")  // simple lookup
private String bootstrapServers;

@Value("${kafka.topics.raw-telemetry:raw-telemetry}")  // with default value after ':'
private String rawTelemetryTopic;
```

The expression inside `${}` is a **property key** — dot-separated path in `application.yml`.

### In Aerostream

`KafkaProducerConfig` uses `@Value` on fields:
```java
@Value("${spring.kafka.bootstrap-servers}")
private String bootstrapServers;

@Value("${spring.kafka.properties.schema.registry.url}")
private String schemaRegistryUrl;
```

`TelemetrySimulator` uses it on a field too:
```java
@Value("${kafka.topics.raw-telemetry:raw-telemetry}")
private String rawTelemetryTopic;
```

`DlqPublisher` uses it in the **constructor** — a completely valid and testable approach:
```java
public DlqPublisher(
    @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
    ...
) { ... }
```

### When to use @Value vs. @ConfigurationProperties

Use `@Value` for **one or two isolated values**. If you have a logical group of settings (like all `simulator.*` properties), use `@ConfigurationProperties` (see next section).

---

## 8. Type-Safe Configuration: @ConfigurationProperties

`@ConfigurationProperties` binds an entire **namespace** of `application.yml` to a Java object. It's the preferred approach for structured configuration.

### How it works

```yaml
# application.yml
simulator:
  events-per-second: 500
  num-cars: 20
  session-id: "RACE_2024_MONZA_R1"
  enabled: false
```

```java
@Data  // Lombok: generates getters/setters Spring needs to bind values
@ConfigurationProperties(prefix = "simulator")
public class SimulatorProperties {
    private int eventsPerSecond;   // maps to simulator.events-per-second
    private int numCars;           // maps to simulator.num-cars
    private String sessionId;      // maps to simulator.session-id
    private boolean enabled;       // maps to simulator.enabled
}
```

Spring handles **relaxed binding**: `events-per-second` in YAML → `eventsPerSecond` in Java. Hyphens, underscores, and camelCase all map correctly.

### Registering the bean

`@ConfigurationProperties` alone doesn't make the class a bean. You activate it on the main class:

```java
@SpringBootApplication
@EnableConfigurationProperties(SimulatorProperties.class)  // registers SimulatorProperties as a bean
public class AerostreamProducerApplication { ... }
```

Now `SimulatorProperties` is injectable anywhere:
```java
// TelemetrySimulator constructor
public TelemetrySimulator(..., SimulatorProperties simulatorProperties, ...) {
    this.eventsPerSecond = simulatorProperties.getEventsPerSecond();
    this.numCars = simulatorProperties.getNumCars();
}
```

### Why @ConfigurationProperties beats @Value for groups

| | `@Value` | `@ConfigurationProperties` |
|--|---------|--------------------------|
| Validation (`@NotNull`, etc.) | No | Yes (`@Validated`) |
| Type conversion | Basic | Rich (Duration, DataSize, enums, lists) |
| IDE auto-complete | No | Yes (via metadata processor) |
| Refactoring | Fragile (string keys) | Safe (Java field rename) |
| Testing | Harder | Easy (just `new SimulatorProperties()`) |

---

## 9. Spring Profiles

Profiles let you have **different configurations for different environments** without changing code.

### Defining a profile-specific YAML

```
application.yml           ← always loaded
application-docker.yml    ← loaded when "docker" profile is active
application-production.yml← loaded when "production" profile is active
```

In Aerostream, `application-docker.yml` overrides the Kafka bootstrap servers and Schema Registry URL to point at container hostnames instead of `localhost`:

```yaml
# application-docker.yml
spring:
  kafka:
    bootstrap-servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
    properties:
      schema.registry.url: http://schema-registry:8081
```

### Activating a profile

**In code (Dockerfile):**
```dockerfile
ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "/app/app.jar"]
```

**Via environment variable (docker-compose.yml):**
```yaml
environment:
  SPRING_PROFILES_ACTIVE: docker
```

**For local dev:** Run with VM option `-Dspring.profiles.active=local` or add `spring.profiles.active=local` in `application.yml`.

### @Profile — conditional beans

```java
@RestController
@RequestMapping("/api/test")
@Profile("!production")  // This bean only exists when NOT in "production" profile
public class TestController { ... }
```

When `production` profile is active, Spring skips creating `TestController` entirely — the test endpoints disappear from the running app. This is a safe, clean way to include debug/test tooling in development without it leaking to production.

---

## 10. application.yml — The Config File

Spring Boot reads `src/main/resources/application.yml` automatically. YAML uses indentation to represent hierarchy, which maps to dot-separated keys in Spring.

### Reading Aerostream's config

```yaml
spring:                                   # spring.* namespace — Spring Boot's own config
  application:
    name: aerostream-producer             # spring.application.name
  kafka:
    bootstrap-servers: localhost:9092,... # spring.kafka.bootstrap-servers
    properties:
      schema.registry.url: http://...     # spring.kafka.properties.schema.registry.url

server:
  port: 8090                              # Tomcat listens on 8090, not the default 8080

management:                               # Actuator config
  endpoints:
    web:
      exposure:
        include: health,prometheus,metrics,info  # which endpoints to expose over HTTP

kafka:                                    # custom namespace (not built-in Spring)
  topics:
    raw-telemetry: raw-telemetry          # kafka.topics.raw-telemetry

simulator:                                # bound to SimulatorProperties via @ConfigurationProperties
  events-per-second: 500
  num-cars: 20
  session-id: "RACE_2024_MONZA_R1"
  enabled: false
```

### Property resolution order (highest wins)

1. Command-line arguments (`--server.port=9000`)
2. Environment variables (`SERVER_PORT=9000`, Spring converts `_` → `.` and lowercases)
3. Profile-specific YAML (`application-docker.yml`)
4. Default YAML (`application.yml`)
5. Default values in `@Value("${key:default}")` expressions

This is why `application-docker.yml` can safely override just the Kafka URLs — everything else falls through from `application.yml`.

---

## 11. The Web Layer

### @RestController

`@RestController` combines `@Controller` and `@ResponseBody`. Every method return value is automatically serialized to JSON and written to the HTTP response.

```java
@RestController
@RequestMapping("/api/simulator")  // base path for all methods in this class
public class SimulatorController {

    private final TelemetrySimulator simulator;

    public SimulatorController(TelemetrySimulator simulator) {
        this.simulator = simulator;  // constructor injection
    }

    @GetMapping("/status")   // HTTP GET /api/simulator/status
    public ResponseEntity<SimulatorStatus> getStatus() {
        return ResponseEntity.ok(simulator.getStatus());
    }

    @PostMapping("/start")   // HTTP POST /api/simulator/start
    public ResponseEntity<String> startSimulator(
        @RequestParam(required = false) Integer eventsPerSecond  // ?eventsPerSecond=1000
    ) { ... }
}
```

### Common mapping annotations

| Annotation | HTTP method | Example usage |
|-----------|------------|--------------|
| `@GetMapping("/path")` | GET | Fetch data, read status |
| `@PostMapping("/path")` | POST | Trigger action, send data |
| `@PutMapping("/path")` | PUT | Update a resource |
| `@DeleteMapping("/path")` | DELETE | Delete a resource |
| `@RequestMapping("/path")` | Any | Used at class level for base path |

### @RequestParam

Extracts query string parameters from the URL:

```
POST /api/simulator/start?eventsPerSecond=1000
                          ^^^^^^^^^^^^^^^^^
```

```java
@PostMapping("/start")
public ResponseEntity<String> startSimulator(
    @RequestParam(required = false) Integer eventsPerSecond
) { ... }
```

`required = false` means the parameter is optional; Spring passes `null` if it's absent.

### ResponseEntity

`ResponseEntity<T>` wraps your response body and lets you control the HTTP status code:

```java
ResponseEntity.ok(body)                    // 200 OK
ResponseEntity.badRequest().body("error")  // 400 Bad Request
ResponseEntity.noContent().build()         // 204 No Content
```

---

## 12. Application Lifecycle — @EventListener

Spring fires events at key points in the application lifecycle. You can listen to any event with `@EventListener`.

```java
@Component
public class TelemetrySimulator {

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // Called after ALL beans are initialized and the app is fully started.
        // Safe to use any injected bean here.
        if (simulatorProperties.isEnabled()) {
            start();
        }
    }
}
```

`ApplicationReadyEvent` fires after:
- All beans are created and wired.
- Embedded Tomcat is running.
- Command-line runners have executed.

This is the right place to kick off background tasks or auto-start logic. Doing the same thing in a constructor or `@PostConstruct` is risky because other beans may not be ready yet.

### Common lifecycle events

| Event | When it fires |
|-------|--------------|
| `ApplicationStartingEvent` | Very early, before context is created |
| `ApplicationEnvironmentPreparedEvent` | After environment is set up |
| `ApplicationReadyEvent` | App is fully started and ready to serve requests ✓ |
| `ContextClosedEvent` | App is shutting down |

---

## 13. Spring Kafka

Spring Kafka wraps the Apache Kafka client library and integrates it with Spring's DI and configuration system.

### The two key abstractions

**ProducerFactory** — creates and configures Kafka `Producer` instances:

```java
@Bean
public ProducerFactory<String, TelemetryEvent> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // exactly-once per partition
    config.put(ProducerConfig.ACKS_CONFIG, "all");               // wait for all replicas
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");   // compress batches
    return new DefaultKafkaProducerFactory<>(config);
}
```

**KafkaTemplate** — the high-level API for sending messages. You almost never touch `ProducerFactory` directly in application code:

```java
@Bean
public KafkaTemplate<String, TelemetryEvent> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
}
```

Usage in `TelemetrySimulator`:
```java
ListenableFuture<SendResult<String, TelemetryEvent>> future =
    kafkaTemplate.send(rawTelemetryTopic, carId, event);
```

`send()` is non-blocking — it returns a future. You can attach callbacks for success/failure handling (which is how `DlqPublisher` is involved when sends fail).

### Key Kafka producer settings used in this project

| Config | Value | Meaning |
|--------|-------|---------|
| `acks=all` | `"all"` | Message is acknowledged only when all in-sync replicas have it |
| `enable.idempotence` | `true` | Exactly-once delivery per partition, no duplicates on retry |
| `compression.type` | `lz4` | Compress batches before sending — reduces network I/O |
| `batch.size` | 65536 | Accumulate up to 64 KB before sending — improves throughput |
| `linger.ms` | 5 | Wait up to 5 ms to fill a batch — throughput vs. latency trade-off |
| `partitioner.class` | `TelemetryPartitioner` | Custom partitioning logic based on car ID |

### Custom Partitioner

`TelemetryPartitioner` implements Kafka's `Partitioner` interface (not a Spring bean — Kafka instantiates it internally). It routes events so that all data for a given car always goes to the same partition, ensuring ordered processing per car downstream.

### Why Spring Kafka over raw Kafka client?

- `KafkaTemplate` handles thread safety, producer lifecycle, and transactions.
- Integrates with Spring's transaction management (`@Transactional`).
- Error handling and retry callbacks.
- Test support (`EmbeddedKafka` for integration tests).

---

## 14. Spring Boot Actuator and Micrometer Metrics

### Spring Boot Actuator

Actuator adds production-ready endpoints to your app automatically when you add `spring-boot-starter-actuator` to `pom.xml`.

In `application.yml`:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,metrics,info
  endpoint:
    health:
      show-details: always
  prometheus:
    metrics:
      export:
        enabled: true
```

| Endpoint | URL | What it shows |
|----------|-----|--------------|
| `/actuator/health` | `GET` | App health, Kafka connectivity |
| `/actuator/metrics` | `GET` | All registered metrics |
| `/actuator/prometheus` | `GET` | Metrics in Prometheus text format |
| `/actuator/info` | `GET` | App name, version, git info |

Prometheus scrapes `/actuator/prometheus` every 15 seconds (configured in `infra/prometheus/prometheus.yml`), and Grafana queries Prometheus to build dashboards.

### Micrometer — Application Metrics

Micrometer is Spring Boot's metrics facade — a vendor-neutral API that can export to Prometheus, Datadog, CloudWatch, etc.

The key type is **`MeterRegistry`**, which Spring Boot auto-configures and injects:

```java
@Component
public class TelemetrySimulator {
    private final MeterRegistry meterRegistry;

    public TelemetrySimulator(..., MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        // Register a counter that increments each time an event is sent
        Counter.builder("telemetry.events.sent")
               .description("Total telemetry events sent to Kafka")
               .register(meterRegistry);
    }
}
```

### Types of meters

| Type | Use case | Aerostream example |
|------|---------|-------------------|
| `Counter` | Things that only go up (events sent, errors) | Events sent to Kafka, DLQ messages |
| `Gauge` | Current value (active cars, queue depth) | Number of simulated cars |
| `Timer` | Duration of operations (request latency) | Kafka send duration |
| `DistributionSummary` | Distribution of values (payload sizes) | — |

You can query any metric via `/actuator/metrics/telemetry.events.sent`.

---

## 15. Lombok — The Spring Developer's Shorthand

Lombok is a Java annotation processor that generates boilerplate code at compile time. It's not part of Spring, but it's used extensively in Spring projects.

### Annotations used in Aerostream

| Annotation | What it generates | Used on |
|-----------|------------------|---------|
| `@Data` | Getters, setters, `equals`, `hashCode`, `toString` | `SimulatorProperties`, `SimulatorStatus`, `CarState` |
| `@Builder` | Builder pattern (`MyClass.builder().field(x).build()`) | `SimulatorStatus`, `CarState` |
| `@Slf4j` | `private static final Logger log = LoggerFactory.getLogger(...)` | `KafkaProducerConfig`, `DlqPublisher`, `TelemetrySimulator`, etc. |
| `@RequiredArgsConstructor` | Constructor for all `final` fields | `PoisonPillGenerator`, `TestController` |

### How @RequiredArgsConstructor enables Spring injection

```java
@Component
@RequiredArgsConstructor   // Lombok generates this constructor:
public class PoisonPillGenerator {
    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final DlqPublisher dlqPublisher;

    // Generated by Lombok — Spring sees this and injects the two beans:
    // public PoisonPillGenerator(KafkaTemplate kafkaTemplate, DlqPublisher dlqPublisher) {
    //     this.kafkaTemplate = kafkaTemplate;
    //     this.dlqPublisher = dlqPublisher;
    // }
}
```

Because Lombok generates the constructor before Spring processes the class, Spring sees a single constructor and uses it for injection — exactly as if you'd written it yourself.

### @Slf4j — logging

```java
@Slf4j
@Component
public class TelemetrySimulator {
    // Lombok injects: private static final Logger log = LoggerFactory.getLogger(TelemetrySimulator.class);

    public void sendEvent() {
        log.info("Sending event for car {}", carId);
        log.warn("Slow send: {} ms", elapsed);
        log.error("Failed to send event", exception);
    }
}
```

Log levels: `TRACE < DEBUG < INFO < WARN < ERROR`. Configured in `application.yml`:
```yaml
logging:
  level:
    com.aerostream: INFO   # show INFO and above for our code
    org.apache.kafka: WARN # suppress Kafka's verbose DEBUG/INFO logs
```

---

## 16. Circular Dependencies and @Lazy

### The problem

A **circular dependency** occurs when Bean A needs Bean B, and Bean B needs Bean A. Spring can't create either one first:

```
TelemetrySimulator → (needs) → DlqPublisher
DlqPublisher       → (needs) → TelemetrySimulator  ← circular!
```

In Aerostream, `DlqPublisher` needs to call `simulator.setDlqPublisher(this)` during construction (to register itself back with the simulator). But `TelemetrySimulator` depends on `DlqPublisher` too.

### The @Lazy solution

```java
@Component
public class DlqPublisher {
    public DlqPublisher(
        @Lazy TelemetrySimulator simulator,  // @Lazy breaks the cycle
        ...
    ) {
        simulator.setDlqPublisher(this);
    }
}
```

`@Lazy` on a constructor parameter tells Spring: *"Don't create `TelemetrySimulator` right now. Give me a proxy. When I actually call a method on it, create the real bean then."*

This breaks the cycle:
1. Spring starts creating `DlqPublisher`.
2. For `TelemetrySimulator`, it creates a **lazy proxy** (no real object yet).
3. `DlqPublisher` finishes construction with the proxy.
4. Later, Spring creates the real `TelemetrySimulator`.
5. When `DlqPublisher` calls `simulator.setDlqPublisher(this)`, the proxy delegates to the now-real bean.

> **Design note:** Circular dependencies often signal that responsibilities need to be split. If you see them, consider whether an event or a third bean (a mediator) can decouple the two.

---

## 17. Spring Boot Starters — How Auto-Configuration Works

### What is a starter?

A starter is a Maven artifact that bundles:
1. **Dependency coordinates** — all the jars you need, at compatible versions.
2. **Auto-configuration classes** — Spring Boot code that creates beans automatically.

When you add `spring-boot-starter-web` to `pom.xml`, Spring Boot detects it on the classpath and fires `WebMvcAutoConfiguration`, which creates a `DispatcherServlet`, configures Jackson for JSON, starts Tomcat, etc. — all without any code from you.

### Starters in Aerostream's pom.xml

| Starter / Dependency | What it auto-configures |
|---------------------|------------------------|
| `spring-boot-starter-web` | Embedded Tomcat, Spring MVC, Jackson JSON, `DispatcherServlet` |
| `spring-kafka` | `KafkaTemplate` (if `spring.kafka.bootstrap-servers` is set), consumer factory |
| `spring-boot-starter-actuator` | Health endpoint, metrics, info endpoint |
| `micrometer-registry-prometheus` | Prometheus `MeterRegistry`, `/actuator/prometheus` endpoint |
| `spring-boot-starter-test` | JUnit 5, Mockito, `MockMvc` for tests |

### Why the project overrides auto-configuration for Kafka

Spring Boot would auto-configure a `KafkaTemplate<String, String>` by default. But Aerostream needs `KafkaTemplate<String, TelemetryEvent>` with Avro serialization, a custom partitioner, and specific producer settings. So `KafkaProducerConfig` manually defines the `ProducerFactory` and `KafkaTemplate` beans — Spring Boot sees your beans and backs off from its defaults (this is called **conditional auto-configuration**).

---

## 18. Putting It All Together

Here is the complete bean dependency graph for Aerostream and how each Spring concept ties in:

```
AerostreamProducerApplication (@SpringBootApplication)
│
├── @EnableConfigurationProperties ──► SimulatorProperties (@ConfigurationProperties)
│                                         └── binds simulator.* from application.yml
│
├── KafkaProducerConfig (@Configuration)
│   ├── @Value("${spring.kafka.bootstrap-servers}") ──► reads application.yml
│   ├── @Bean ProducerFactory ──────────────────────► wraps Confluent KafkaAvroSerializer
│   └── @Bean KafkaTemplate ────────────────────────► used by TelemetrySimulator, PoisonPillGenerator
│
├── SchemaRegistryConfig (@Configuration)
│   └── @Bean SchemaRegistryClient ─────────────────► used by SchemaRegistrationTest
│
├── TelemetrySimulator (@Component)
│   ├── injects: PhysicsEngine, KafkaTemplate, SimulatorProperties, MeterRegistry
│   ├── @Value("${kafka.topics.raw-telemetry:raw-telemetry}")
│   ├── @EventListener(ApplicationReadyEvent) ──────► auto-starts if simulator.enabled=true
│   └── setDlqPublisher() ──────────────────────────► called by DlqPublisher after creation
│
├── PhysicsEngine (@Component)
│   └── injects: nothing
│
├── DlqPublisher (@Component)
│   ├── injects: @Lazy TelemetrySimulator (breaks circular dep), MeterRegistry
│   ├── @Value("${spring.kafka.bootstrap-servers}")
│   └── builds its own KafkaTemplate<String, byte[]> manually (for raw DLQ bytes)
│
├── SimulatorController (@RestController)
│   ├── injects: TelemetrySimulator
│   └── HTTP: GET /api/simulator/status, POST /api/simulator/start, POST /api/simulator/stop
│
├── PoisonPillGenerator (@Component)  [@RequiredArgsConstructor]
│   └── injects: KafkaTemplate, DlqPublisher
│
└── TestController (@RestController, @Profile("!production"))
    ├── injects: PoisonPillGenerator
    └── HTTP: POST /api/test/poison-pill  (only active outside production profile)
```

### Key concepts recap, one line each

| Concept | One-line summary |
|---------|-----------------|
| **ApplicationContext** | Spring's smart registry — creates and wires all beans |
| **@Component / @Bean** | "Hey Spring, manage this object" |
| **Constructor injection** | Declare what you need; Spring delivers it |
| **@Configuration + @Bean** | A class that defines beans programmatically |
| **@Value** | Pull a single property from application.yml into a field |
| **@ConfigurationProperties** | Bind an entire YAML namespace to a typed Java class |
| **Profiles** | Different config sets for different environments |
| **@RestController** | A bean that handles HTTP requests and returns JSON |
| **@EventListener** | Run code at a specific point in the app lifecycle |
| **KafkaTemplate** | Spring's thread-safe, high-level Kafka producer API |
| **MeterRegistry** | Register and update metrics for Prometheus to scrape |
| **@Lazy** | Defer bean creation to break circular dependencies |
| **Starters** | Bundled dependencies + auto-configuration in one import |

---

## Further Reading

- [Spring Boot Reference Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/)
- [Spring Framework Core — IoC Container](https://docs.spring.io/spring-framework/reference/core/beans.html)
- [Spring for Apache Kafka](https://docs.spring.io/spring-kafka/reference/)
- [Micrometer Concepts](https://micrometer.io/docs/concepts)
- [Baeldung Spring Boot Tutorials](https://www.baeldung.com/spring-boot) — practical examples for every concept above
