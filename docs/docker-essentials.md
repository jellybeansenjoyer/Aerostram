# Docker Essentials — Aerostream Edition

> A ground-up guide to Docker concepts, explained through the real files in this project.
> Every concept is anchored to something you can open and read right now.

---

## Table of Contents

1. [The Core Problem Docker Solves](#1-the-core-problem-docker-solves)
2. [Containers vs. Virtual Machines](#2-containers-vs-virtual-machines)
3. [Images — The Blueprint](#3-images--the-blueprint)
4. [The Dockerfile — Building Your Own Image](#4-the-dockerfile--building-your-own-image)
5. [Multi-Stage Builds — The Producer's Dockerfile](#5-multi-stage-builds--the-producers-dockerfile)
6. [Containers — Running Images](#6-containers--running-images)
7. [Ports — Connecting the Outside World In](#7-ports--connecting-the-outside-world-in)
8. [Volumes — Persistent and Shared Data](#8-volumes--persistent-and-shared-data)
9. [Networks — How Containers Talk to Each Other](#9-networks--how-containers-talk-to-each-other)
10. [Environment Variables and .env Files](#10-environment-variables-and-env-files)
11. [Docker Compose — Orchestrating the Whole Stack](#11-docker-compose--orchestrating-the-whole-stack)
12. [Service Dependencies and Health Checks](#12-service-dependencies-and-health-checks)
13. [Restart Policies](#13-restart-policies)
14. [The `command` Override](#14-the-command-override)
15. [`expose` vs. `ports`](#15-expose-vs-ports)
16. [Essential Docker CLI Commands](#16-essential-docker-cli-commands)
17. [The Aerostream Stack — Full Picture](#17-the-aerostream-stack--full-picture)
18. [Future Scope — Concepts You'll Need Next](#18-future-scope--concepts-youll-need-next)

---

## 1. The Core Problem Docker Solves

Imagine you build a Spring Boot app that works perfectly on your MacBook. You hand it to a teammate. Their machine has a different JDK version, different environment variables, different OS libraries. It breaks. The classic "works on my machine" problem.

Docker solves this by bundling **your application AND everything it needs to run** — the JDK, the OS libraries, the config files — into a single self-contained unit called a **container**. The container runs identically on any machine that has Docker installed.

In Aerostream, every service (Kafka, Schema Registry, Prometheus, Grafana, your Spring Boot producer) runs inside its own container. A developer can clone the repo and spin up the entire platform with one command:

```bash
docker compose up -d
```

No manual installation of Kafka, no JDK version mismatch, no "which Grafana version did you use?"

---

## 2. Containers vs. Virtual Machines

Both isolate software, but they work differently:

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│          VIRTUAL MACHINE        │    │            CONTAINER            │
│  ┌──────────────────────────┐   │    │  ┌──────┐  ┌──────┐  ┌──────┐  │
│  │     Your App + JDK       │   │    │  │App+  │  │App+  │  │App+  │  │
│  │  Full Guest OS (Linux)   │   │    │  │Libs  │  │Libs  │  │Libs  │  │
│  └──────────────────────────┘   │    │  └──────┘  └──────┘  └──────┘  │
│  ┌──────────────────────────┐   │    │  ─────── Shared OS Kernel ───── │
│  │      Hypervisor          │   │    └─────────────────────────────────┘
│  └──────────────────────────┘   │
│  Host OS + Physical Hardware    │
└─────────────────────────────────┘
```

| Aspect | Virtual Machine | Container |
|--------|----------------|-----------|
| Startup time | Minutes (boots a full OS) | Seconds (process starts) |
| Size | Gigabytes (full OS image) | Megabytes |
| Isolation | Complete (own OS kernel) | Process-level (shared kernel) |
| Use case | Full OS-level isolation | App packaging and deployment |

Containers are **not** VMs. They are isolated processes running on the host OS kernel. This makes them fast and lightweight, which is why Aerostream can run 9 containers simultaneously on a developer laptop without it grinding to a halt.

---

## 3. Images — The Blueprint

A **Docker image** is a read-only template — a snapshot of a filesystem at a point in time. It contains:
- The OS base (e.g. Alpine Linux, Ubuntu)
- Installed software (JDK, Kafka binaries)
- Application files (your compiled `.jar`)
- Default configuration

An image is like a **class** in Java. A **container** is like an **instance** of that class — you can run many containers from one image.

### Image naming and tags

```
confluentinc/cp-kafka:7.6.1
│            │        │
│            │        └── tag (version)
│            └─────────── image name
└──────────────────────── registry namespace (Docker Hub username / org)
```

If no tag is specified, Docker uses `:latest`. **Avoid `:latest` in production** — it is mutable and can silently change. Aerostream pins every image to an exact version:

```yaml
# docker-compose.yml
image: confluentinc/cp-kafka:7.6.1          # pinned ✓
image: prom/prometheus:v2.51.0              # pinned ✓
image: grafana/grafana:10.4.0               # pinned ✓
image: provectuslabs/kafka-ui:latest        # latest — acceptable for a dev UI tool
```

### Image layers

Images are built in layers. Each Dockerfile instruction creates a new layer stacked on top of the previous one:

```
Layer 4: COPY app.jar /app/          ← your application
Layer 3: RUN adduser appuser         ← user setup
Layer 2: WORKDIR /app                ← working directory
Layer 1: FROM amazoncorretto:21-alpine ← base OS + JDK
```

Layers are **cached**. If Layer 1-3 haven't changed, Docker reuses them from cache and only rebuilds Layer 4. This is why the producer's Dockerfile copies `pom.xml` before copying `src/` — Maven dependencies change far less often than source code, so the expensive `mvn dependency:go-offline` step is cached.

### Where images come from

| Source | Example |
|--------|---------|
| **Docker Hub** (public registry) | `confluentinc/cp-kafka:7.6.1` — pulled automatically on first `docker compose up` |
| **Built locally** from a Dockerfile | The `producer` service is built from `./producer/Dockerfile` |
| **Private registry** (ECR, GCR, etc.) | Used in production deployments |

---

## 4. The Dockerfile — Building Your Own Image

A `Dockerfile` is a script of instructions for building an image. Each instruction becomes a layer.

### Dockerfile instructions

| Instruction | Purpose |
|------------|---------|
| `FROM` | Set the base image (every Dockerfile starts with this) |
| `WORKDIR` | Set the working directory for subsequent instructions (creates it if missing) |
| `COPY` | Copy files from your machine (build context) into the image |
| `RUN` | Execute a shell command during the build (install packages, compile, etc.) |
| `ENV` | Set environment variables baked into the image |
| `EXPOSE` | Document which port the container will listen on (informational only) |
| `USER` | Switch to a non-root user (security best practice) |
| `ENTRYPOINT` | The command that runs when the container starts (not easily overridden) |
| `CMD` | Default arguments to `ENTRYPOINT`, or the default command if no `ENTRYPOINT` |

### A minimal Dockerfile example

```dockerfile
FROM amazoncorretto:21-alpine     # start from Amazon's JDK 21 on Alpine Linux
WORKDIR /app                      # all subsequent paths are relative to /app
COPY app.jar .                    # copy app.jar into /app/app.jar
EXPOSE 8090                       # document that this app listens on 8090
ENTRYPOINT ["java", "-jar", "app.jar"]   # run on container start
```

### ENTRYPOINT vs. CMD

```dockerfile
ENTRYPOINT ["java", "-jar", "app.jar"]   # fixed — this always runs
CMD ["--server.port=8090"]               # default args — can be overridden
```

- `ENTRYPOINT` is the executable. It cannot be changed without `--entrypoint` flag.
- `CMD` provides default arguments. You can replace them with: `docker run myimage --server.port=9000`.
- Using both together is the most flexible pattern.

In the producer's Dockerfile, the Spring profile is baked into `ENTRYPOINT`:
```dockerfile
ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "app.jar"]
```

---

## 5. Multi-Stage Builds — The Producer's Dockerfile

The producer uses a **multi-stage build** — one of the most important Docker patterns.

```dockerfile
# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-amazoncorretto-21 AS builder   # heavy image: Maven + JDK 21
WORKDIR /build

# Cache dependencies separately from source code
COPY pom.xml .
RUN mvn dependency:go-offline -q                 # download all dependencies

COPY src ./src
RUN mvn clean package -DskipTests -q             # compile → produces .jar

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM amazoncorretto:21-alpine                    # lightweight image: JDK only
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup   # non-root user

COPY --from=builder /build/target/aerostream-producer-*.jar app.jar   # grab only the .jar

USER appuser          # drop root privileges
EXPOSE 8090
ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "app.jar"]
```

### Why two stages?

**Without multi-stage:**
```
Final image = Maven + JDK 21 + source code + compiled .jar + all build tools
                                                             ≈ 700 MB+
```

**With multi-stage:**
```
Stage 1 (builder): Maven + JDK 21 + source code → compiles .jar
Stage 2 (runtime): JDK 21 only + .jar           ← this is the final image
                                                             ≈ 200 MB
```

Only Stage 2 becomes the final image. Stage 1 is a **throwaway build environment** — all the Maven binaries, source files, and build caches are discarded. The `COPY --from=builder` instruction reaches back into Stage 1 and pulls only the artifact you need.

### The dependency caching trick

```dockerfile
COPY pom.xml .
RUN mvn dependency:go-offline -q   # ← downloads all Maven deps (~300 MB)

COPY src ./src                     # ← source files copied AFTER deps
RUN mvn clean package -DskipTests -q
```

Docker builds layers sequentially and caches each one. If `pom.xml` hasn't changed, the `mvn dependency:go-offline` layer is served from cache — skipping the ~2 minute download entirely. Only when you add a new dependency does this layer rebuild.

If you instead did `COPY . .` first, **any** file change (even editing a comment in a `.java` file) would invalidate the cache and re-download all dependencies.

### Security: non-root user

```dockerfile
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
```

Containers run as root by default. If an attacker exploits your app, running as root inside the container gives them more power. Creating a dedicated, unprivileged user is a security best practice. `-S` means "system account" (no password, no home directory).

---

## 6. Containers — Running Images

A container is a running instance of an image. Key concepts:

### Container lifecycle

```
Image ──► docker run ──► Container (Running)
                              │
                         docker stop ──► Container (Stopped) ──► docker start ──► Running
                              │
                         docker rm   ──► Container (Deleted)
```

A stopped container still exists on disk (with its filesystem state). `docker rm` permanently removes it.

### Container filesystem is ephemeral

When a container stops, **any data written inside the container is lost** unless you use a **volume** (see Section 8). This is intentional — containers are meant to be stateless and replaceable.

### Container identity

Every container has:
- A random **ID** (e.g. `a3f9c12b8d4e`)
- A human-readable **name** (set via `container_name` in Compose, or random if not set)

In Aerostream, every service has an explicit name:
```yaml
container_name: kafka-1
container_name: schema-registry
container_name: aerostream-producer
```

This matters because other containers reference each other by name on the shared network (see Section 9).

---

## 7. Ports — Connecting the Outside World In

By default, a container is completely isolated — no external traffic can reach it. Port mapping punches a hole through that isolation.

### Syntax

```yaml
ports:
  - "HOST_PORT:CONTAINER_PORT"
```

```
Your Mac's port 9092       →       kafka-1 container's port 9093
          │                                      │
          └──── port mapping ───────────────────►│
                "9092:9093"
```

The **container port** is what the process inside listens on. The **host port** is what you use from your Mac (or CI machine).

### In Aerostream's docker-compose.yml

```yaml
# kafka-1
ports:
  - "${KAFKA_BROKER_1_EXTERNAL_PORT:-9092}:9093"
```

This reads: "Map host port `9092` (or whatever's in `.env`) to container port `9093`."

Why different ports? Inside the container, Kafka listens on `9093` for external connections (and `9092` for internal inter-broker traffic). Outside, we expose it as `9092` for convenience.

```yaml
# producer
ports:
  - "${PRODUCER_PORT:-8090}:8090"
```

Here host and container port are the same (`8090:8090`). The producer listens on `8090` inside, and we expose the same port outside.

### Variable port values with defaults

```yaml
- "${KAFKA_BROKER_1_EXTERNAL_PORT:-9092}:9093"
#   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^
#   env var, with default 9092           fallback if var is not set
```

`${VAR:-default}` is shell variable expansion. Docker Compose reads `.env` automatically. If `KAFKA_BROKER_1_EXTERNAL_PORT` is set there, it uses that value; otherwise it falls back to `9092`. This lets different developers run on different ports without editing the Compose file.

---

## 8. Volumes — Persistent and Shared Data

Volumes solve the problem of ephemeral container filesystems. There are two kinds used in Aerostream:

### Named Volumes — Managed by Docker

Declared at the top of `docker-compose.yml`:
```yaml
volumes:
  kafka-1-data: {}
  kafka-2-data: {}
  kafka-3-data: {}
  grafana-data: {}
```

Used in a service:
```yaml
kafka-1:
  volumes:
    - kafka-1-data:/var/lib/kafka/data   # named volume : container path
```

Docker manages the storage location on your host (usually under `/var/lib/docker/volumes/`). The data **survives container restarts and recreations**. When you run `docker compose down`, named volumes are preserved. Only `docker compose down -v` deletes them.

This is essential for Kafka — if the broker container restarts, it must find its topic data intact.

### Bind Mounts — Host Directory into Container

```yaml
prometheus:
  volumes:
    - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    #   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^
    #   host path (relative to compose file)  container path               read-only
```

A bind mount maps a **specific directory or file from your host** into the container. Changes on either side are reflected immediately — there is no copy, it's the same filesystem path.

Uses in Aerostream:
- `./infra/prometheus/prometheus.yml` → Prometheus config, read-only so the container can't accidentally modify it.
- `./infra/prometheus/jmx-config` → JMX agent config files for Kafka metrics, shared across Kafka and Prometheus containers.
- `./infra/grafana/provisioning` → Grafana auto-provisioning (datasources, dashboard provider config).
- `./infra/grafana/dashboards` → Grafana dashboard JSON files.

### Named volume vs. Bind mount — when to use which

| Aspect | Named Volume | Bind Mount |
|--------|-------------|-----------|
| Docker manages path | Yes | No (you choose the path) |
| Good for | App data, databases, Kafka logs | Config files you edit locally |
| Survives `down` | Yes | The host file always exists |
| Editable from host easily | Not directly | Yes — just edit the file |

---

## 9. Networks — How Containers Talk to Each Other

### The problem without a shared network

By default, containers are isolated from each other. The Schema Registry needs to connect to the Kafka brokers. Without a network, it can't.

### Creating a shared network

At the top of `docker-compose.yml`:
```yaml
networks:
  aerostream-network:
    driver: bridge
```

Every service joins it:
```yaml
kafka-1:
  networks:
    - aerostream-network

schema-registry:
  networks:
    - aerostream-network
```

### How container DNS works

Within `aerostream-network`, every container is reachable by its **service name** (or `container_name`) as a hostname. Docker runs an internal DNS server that resolves these names.

```yaml
# Schema Registry connects to Kafka brokers using their service names:
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
#                                              ^^^^^^^ hostname = service name in Compose
```

```yaml
# Prometheus scrapes the producer using its container name:
# prometheus.yml:
targets:
  - aerostream-producer:8090   # container_name of the producer service
```

```yaml
# Producer's application-docker.yml:
spring:
  kafka:
    bootstrap-servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
    # These resolve inside Docker — "kafka-1" is the hostname of the kafka-1 container
```

This is why the producer has two different configs:
- `application.yml` uses `localhost:9092` — for running the app directly on your Mac.
- `application-docker.yml` uses `kafka-1:9092` — for running inside Docker where "kafka-1" is a DNS name.

### Bridge network driver

The `bridge` driver is the default and most common. It creates a virtual Ethernet bridge on the host. Containers on the same bridge can reach each other; containers on different bridges cannot (unless explicitly connected).

Other drivers exist (`host`, `overlay` for multi-host Swarm/Kubernetes) but `bridge` covers all single-host use cases.

---

## 10. Environment Variables and .env Files

### Injecting config into containers

```yaml
# docker-compose.yml
grafana:
  environment:
    GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
    GF_USERS_ALLOW_SIGN_UP: "false"
```

The `environment` block sets environment variables inside the container. Processes running inside read them with standard OS env var APIs.

In Spring Boot, environment variables override `application.yml` values. `SPRING_PROFILES_ACTIVE: docker` is equivalent to passing `-Dspring.profiles.active=docker` on the command line.

### The .env file

Docker Compose automatically reads a `.env` file in the same directory as `docker-compose.yml`. Variables defined there are available as `${VAR_NAME}` throughout the Compose file.

```bash
# .env  (never committed to git — contains secrets and machine-specific values)
KAFKA_CLUSTER_ID=AbCdEfGhIjKlMnOp123456==
KAFKA_BROKER_1_EXTERNAL_PORT=9092
GRAFANA_ADMIN_PASSWORD=mysecretpassword
PRODUCER_PORT=8090
SIMULATOR_EVENTS_PER_SECOND=10000
```

```yaml
# docker-compose.yml — these ${...} expressions are replaced with .env values
ports:
  - "${KAFKA_BROKER_1_EXTERNAL_PORT:-9092}:9093"
environment:
  GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
```

### .env.example — the convention

Never commit `.env` because it may contain passwords and API keys. Instead, commit `.env.example` — a template with all variable names but no real values:

```bash
# .gitignore
.env          # ignored — never committed

# .env.example — committed — shows developers what vars they need to set
KAFKA_CLUSTER_ID=        # generate with: bash infra/scripts/init-kafka-storage.sh
GRAFANA_ADMIN_PASSWORD=admin
```

Developers copy it and fill it in:
```bash
cp .env.example .env
```

### Passing env vars to a container vs. baking them into the image

| Method | When to use |
|--------|------------|
| `environment:` in Compose | Runtime config that differs per environment (passwords, host URLs) |
| `ENV` in Dockerfile | Default values baked into the image (rarely used for secrets) |

Never put secrets in the Dockerfile. They would be visible in the image layers to anyone who has the image.

---

## 11. Docker Compose — Orchestrating the Whole Stack

Docker Compose lets you define and run **multi-container applications** in a single YAML file. Without it, you'd run 9 separate `docker run` commands with many flags, manually coordinate startup order, and manage networks and volumes by hand.

### The docker-compose.yml structure

```yaml
networks:      # define shared networks
  ...

volumes:       # define named volumes
  ...

services:      # define each container
  service-name:
    image: ...           # use a pre-built image
    # OR
    build: ...           # build from a Dockerfile

    container_name: ...  # give a predictable name
    hostname: ...        # hostname inside the container's /etc/hosts
    ports: ...           # publish ports to host
    environment: ...     # set env vars
    volumes: ...         # attach storage
    networks: ...        # join networks
    depends_on: ...      # startup ordering
    healthcheck: ...     # how to test if service is healthy
    restart: ...         # what to do if the container crashes
    command: ...         # override the default command
    expose: ...          # document internal ports (no host binding)
```

### build vs. image

```yaml
# Use a pre-built image from a registry:
kafka-1:
  image: confluentinc/cp-kafka:7.6.1

# Build from a local Dockerfile:
producer:
  build:
    context: ./producer    # directory containing the Dockerfile (and build context)
    dockerfile: Dockerfile # relative to context
```

When you run `docker compose up`, Compose builds the producer image first if it doesn't exist, then starts all containers.

### Essential Compose commands

```bash
docker compose up -d              # Start all services in detached (background) mode
docker compose up -d producer     # Start only the producer (and its depends_on chain)
docker compose down               # Stop and remove all containers (volumes kept)
docker compose down -v            # Stop, remove containers AND volumes (data lost!)
docker compose build              # Rebuild images without starting containers
docker compose build --no-cache   # Force full rebuild, no layer cache
docker compose ps                 # List container status
docker compose logs -f            # Follow logs from all services
docker compose logs -f producer   # Follow logs from producer only
docker compose restart producer   # Restart a single service
docker compose exec kafka-1 bash  # Open a shell inside the kafka-1 container
```

---

## 12. Service Dependencies and Health Checks

### depends_on — startup ordering

```yaml
schema-registry:
  depends_on:
    kafka-1: { condition: service_healthy }
    kafka-2: { condition: service_healthy }
    kafka-3: { condition: service_healthy }
```

Without `depends_on`, Compose starts all containers simultaneously. Schema Registry would try to connect to Kafka before it's ready and crash.

`depends_on` with `condition: service_healthy` means: *"Don't start this service until kafka-1, kafka-2, and kafka-3 all pass their health checks."*

There are three condition values:

| Condition | Meaning |
|-----------|---------|
| `service_started` | Container has started (no health check needed) |
| `service_healthy` | Container passes its `healthcheck` |
| `service_completed_successfully` | Container ran and exited with code 0 (for init jobs) |

### healthcheck — defining "healthy"

```yaml
kafka-1:
  healthcheck:
    test: ["CMD-SHELL", "KAFKA_OPTS='' kafka-topics --bootstrap-server localhost:9092 --list"]
    interval: 30s      # run the check every 30 seconds
    timeout: 10s       # if the check takes >10s, it fails
    retries: 10        # after 10 consecutive failures, mark container as unhealthy
    start_period: 30s  # grace period before failures count (allows slow startup)
```

`test` is the command to run inside the container. If it exits with code `0`, the container is healthy. The Kafka health check runs `kafka-topics --list` — if Kafka is up, it responds; if not, the command fails.

```yaml
schema-registry:
  healthcheck:
    test: curl -f http://localhost:8081/subjects || exit 1
    # curl -f exits non-zero on HTTP errors — clean way to check an HTTP endpoint
```

### The producer's dependency chain

```yaml
producer:
  depends_on:
    kafka-1:
      condition: service_healthy    # waits for all 3 Kafka brokers
    kafka-2:
      condition: service_healthy
    kafka-3:
      condition: service_healthy
    schema-registry:
      condition: service_healthy    # and Schema Registry
```

Full startup order:
```
kafka-1, kafka-2, kafka-3 start simultaneously
    │         │         │
    └─────────┴────────►│ pass healthchecks (~60s)
                        │
              schema-registry starts
                        │
                  passes healthcheck
                        │
                  producer starts
```

---

## 13. Restart Policies

```yaml
kafka-ui:
  restart: unless-stopped

producer:
  restart: unless-stopped
```

| Policy | Behavior |
|--------|---------|
| `no` | Never restart (default) |
| `always` | Always restart, even on clean exit |
| `on-failure` | Restart only if exited with non-zero code |
| `unless-stopped` | Always restart unless you explicitly stop it with `docker compose stop` |

`unless-stopped` is the right choice for services that should always be running (like a Kafka UI or your producer). If the container crashes due to an error, Docker automatically restarts it. If you deliberately stop it, it stays stopped.

---

## 14. The `command` Override

```yaml
prometheus:
  command:
    - --config.file=/etc/prometheus/prometheus.yml
    - --storage.tsdb.retention.time=15d
    - --web.enable-lifecycle
```

The `command` key overrides the `CMD` instruction in the Dockerfile (or the image's default command). Here, Prometheus's default command is replaced with explicit flags:
- `--config.file` → where to find `prometheus.yml`
- `--storage.tsdb.retention.time=15d` → keep 15 days of metrics data
- `--web.enable-lifecycle` → allow reloading config via HTTP POST

This is cleaner than a custom Dockerfile for pre-built images — you configure them with arguments rather than rebuilding.

---

## 15. `expose` vs. `ports`

Both appear in the Kafka broker services:

```yaml
kafka-1:
  ports:
    - "${KAFKA_BROKER_1_EXTERNAL_PORT:-9092}:9093"   # accessible from your Mac
  expose:
    - "7071"                                          # only accessible within the Docker network
```

| Keyword | Host can access? | Other containers can access? | Purpose |
|---------|-----------------|------------------------------|---------|
| `ports` | Yes (mapped to host) | Yes | External access + internal access |
| `expose` | No | Yes | Internal services only — no host binding |

Port `7071` is the JMX metrics port scraped by Prometheus. Prometheus is on the same `aerostream-network`, so it can reach `kafka-1:7071` directly. There's no reason to expose this to your host machine — `expose` keeps it internal-only.

Think of `expose` as documentation — it tells Docker (and other developers) "this port is available internally" without opening it to the outside.

---

## 16. Essential Docker CLI Commands

### Working with images

```bash
docker images                          # list all local images
docker pull prom/prometheus:v2.51.0    # download an image from Docker Hub
docker rmi prom/prometheus:v2.51.0     # remove a local image
docker build -t myapp:1.0 .            # build an image from Dockerfile in current dir
docker build --no-cache -t myapp:1.0 . # build without layer cache (full rebuild)
```

### Working with containers

```bash
docker ps                              # list running containers
docker ps -a                           # list all containers (including stopped)
docker run -d -p 8090:8090 myapp:1.0   # run a container in background
docker stop kafka-1                    # gracefully stop (SIGTERM then SIGKILL)
docker start kafka-1                   # start a stopped container
docker rm kafka-1                      # delete a stopped container
docker rm -f kafka-1                   # force-delete a running container
```

### Inspecting containers

```bash
docker logs kafka-1                    # show container logs
docker logs -f kafka-1                 # follow (stream) logs
docker logs --tail 100 kafka-1         # last 100 lines
docker exec -it kafka-1 bash           # open interactive shell inside container
docker exec kafka-1 cat /etc/hosts     # run a single command and exit
docker inspect kafka-1                 # full JSON config/state of the container
docker stats                           # live CPU/memory usage of all containers
```

### Working with volumes

```bash
docker volume ls                       # list all volumes
docker volume inspect kafka-1-data     # show volume details (mountpoint on host)
docker volume rm kafka-1-data          # delete a volume (container must be stopped)
docker volume prune                    # delete all unused volumes
```

### Cleanup commands (use carefully)

```bash
docker system prune                    # remove stopped containers, unused images, networks
docker system prune -a                 # also remove unused images (not just dangling)
docker system prune -a --volumes       # nuclear option — removes everything
```

### Docker Compose shortcuts

```bash
docker compose up -d                   # start all services
docker compose up -d --build           # rebuild images and start
docker compose down                    # stop and remove containers
docker compose ps                      # status of all services
docker compose logs -f service-name    # follow a service's logs
docker compose exec service-name bash  # open shell in a running service
docker compose top                     # show running processes in all containers
```

---

## 17. The Aerostream Stack — Full Picture

Here is a complete map of every service, what image it uses, how it connects, and what data it owns:

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          aerostream-network (bridge)                            ║
║                                                                                  ║
║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                          ║
║  │   kafka-1    │  │   kafka-2    │  │   kafka-3    │                          ║
║  │ cp-kafka:7.6 │  │ cp-kafka:7.6 │  │ cp-kafka:7.6 │                          ║
║  │ :9092 (ext)  │  │ :9094 (ext)  │  │ :9096 (ext)  │                          ║
║  │ vol: k1-data │  │ vol: k2-data │  │ vol: k3-data │                          ║
║  │ jmx :7071    │  │ jmx :7071    │  │ jmx :7071    │                          ║
║  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                          ║
║         │                 │                  │                                   ║
║         └─────────────────┴──────────────────┘                                   ║
║                           │ kafka-1:9092, kafka-2:9092, kafka-3:9092             ║
║                           │                                                      ║
║         ┌─────────────────┴───────────────────────────────┐                     ║
║         │                                                  │                     ║
║  ┌──────▼──────────┐                           ┌──────────▼────────┐            ║
║  │ schema-registry │                           │    prometheus     │            ║
║  │  cp-sr:7.6.1    │                           │  prom:v2.51.0     │            ║
║  │  :8081 (ext)    │◄── depends on kafka  ────►│  :9090 (ext)      │            ║
║  └──────┬──────────┘                           │  scrapes :7071    │            ║
║         │                                      │  scrapes :8090    │            ║
║         │                                      └──────────┬────────┘            ║
║         │                                                 │                     ║
║  ┌──────▼──────────────────────────┐          ┌──────────▼────────┐            ║
║  │        producer                 │          │      grafana      │            ║
║  │  built from ./producer/         │          │  grafana:10.4.0   │            ║
║  │  Dockerfile (multi-stage)       │          │  :3000 (ext)      │            ║
║  │  :8090 (ext)                    │          │  vol: grafana-data │            ║
║  │  profile: docker                │          │  bind: ./infra/   │            ║
║  │  /actuator/prometheus → metrics │          │  grafana/         │            ║
║  └─────────────────────────────────┘          └───────────────────┘            ║
║                                                                                  ║
║  ┌──────────────────┐                                                            ║
║  │     kafka-ui     │                                                            ║
║  │  kafka-ui:latest │                                                            ║
║  │  :8080 (ext)     │                                                            ║
║  └──────────────────┘                                                            ║
╚══════════════════════════════════════════════════════════════════════════════════╝

External ports (accessible from your Mac):
  localhost:9092  → kafka-1     (Kafka broker)
  localhost:9094  → kafka-2     (Kafka broker)
  localhost:9096  → kafka-3     (Kafka broker)
  localhost:8081  → schema-registry
  localhost:8080  → kafka-ui    (browser dashboard)
  localhost:8090  → producer    (Spring Boot app + Actuator)
  localhost:9090  → prometheus  (metrics DB)
  localhost:3000  → grafana     (dashboards, login: admin/admin)
```

---

## 18. Future Scope — Concepts You'll Need Next

The project's `.env.example` already hints at future phases (PostgreSQL for Phase 3 CDC, ML consumer for Phase 5). Here are the Docker concepts you'll encounter:

### Docker Registry — Sharing and Deploying Images

Right now, the producer image is only built locally. In production, you push it to a **registry**:

```bash
docker build -t myorg/aerostream-producer:1.0.0 ./producer
docker push myorg/aerostream-producer:1.0.0       # push to Docker Hub
# or push to AWS ECR, Google GCR, GitHub Container Registry, etc.
```

In `docker-compose.yml`, replace `build:` with `image:` pointing to your registry:
```yaml
producer:
  image: myorg/aerostream-producer:1.0.0   # pulled from registry in CI/CD
```

### Resource Limits — Preventing One Container from Starving Others

When you add more services (PostgreSQL, Flink, ML consumers), you'll want to cap resource usage:

```yaml
producer:
  deploy:
    resources:
      limits:
        cpus: "2.0"      # max 2 CPU cores
        memory: 512M     # max 512 MB RAM
      reservations:
        cpus: "0.5"      # guaranteed 0.5 cores
        memory: 256M     # guaranteed 256 MB
```

### Docker Secrets — Proper Secret Management

The `.env` approach is fine for local dev. In production, use Docker Secrets (or a vault) instead of environment variables for passwords:

```yaml
secrets:
  db_password:
    file: ./secrets/db_password.txt

services:
  postgres:
    secrets:
      - db_password
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
```

Secrets are mounted as files, not env vars — harder to accidentally log or leak.

### `profiles` in Compose — Conditional Services

Phase 3 will add PostgreSQL, Phase 5 adds an ML consumer. You don't want to start those when working on Phase 2. Compose profiles let you opt-in to services:

```yaml
services:
  postgres:
    profiles: ["phase3", "full"]   # only starts when you run: docker compose --profile phase3 up

  ml-consumer:
    profiles: ["phase5", "full"]
```

```bash
docker compose up -d                          # starts phase 1+2 services only
docker compose --profile phase3 up -d         # also starts postgres
docker compose --profile full up -d           # starts everything
```

### init containers — One-Time Setup Jobs

The project currently uses shell scripts for initialization (`init-kafka-storage.sh`, `create-topics.sh`). As the project grows, you can replace these with init containers:

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.6.1
  depends_on:
    kafka-1: { condition: service_healthy }
  command: >
    kafka-topics --create --topic raw-telemetry
    --bootstrap-server kafka-1:9092
    --partitions 12 --replication-factor 3
  restart: "no"    # run once and exit
```

### Networking — Multiple Compose Files

As the project grows into microservices, you may split into multiple Compose files and use a shared external network:

```bash
# Create a shared network once:
docker network create aerostream-network

# Each service group joins it:
# docker-compose.kafka.yml
networks:
  aerostream-network:
    external: true   # don't create, join the existing one
```

### The Path to Kubernetes

Docker Compose is the right tool for local development and small deployments. As Aerostream scales, you'll move toward **Kubernetes** (K8s):

| Concept | Docker Compose equivalent | Kubernetes equivalent |
|---------|--------------------------|----------------------|
| Service | `services:` entry | `Deployment` + `Service` |
| Named volume | `volumes:` | `PersistentVolumeClaim` |
| Network | `networks:` | Namespace + `NetworkPolicy` |
| Health check | `healthcheck:` | `livenessProbe` + `readinessProbe` |
| Env var / .env | `environment:` + `.env` | `ConfigMap` + `Secret` |
| Restart policy | `restart:` | `restartPolicy` in Pod spec |
| Resource limits | `deploy.resources` | `resources.limits` in Pod spec |
| Profiles | `profiles:` | Namespace / Helm chart values |

The Docker concepts you've learned here map **directly** to Kubernetes — the vocabulary changes, but the mental models are identical.

---

## Quick Reference

| Concept | Key instruction / command | Where in Aerostream |
|---------|--------------------------|---------------------|
| Build image | `FROM`, `COPY`, `RUN`, `ENTRYPOINT` | `producer/Dockerfile` |
| Multi-stage build | `AS builder`, `COPY --from=builder` | `producer/Dockerfile` |
| Run image | `docker compose up -d` | `docker-compose.yml` |
| Publish port | `ports: "HOST:CONTAINER"` | Every service |
| Internal-only port | `expose: "PORT"` | kafka-1/2/3 port 7071 |
| Persistent data | Named volume `vol-name:/path` | kafka-*-data, grafana-data |
| Config file mount | Bind mount `./host/path:/container/path:ro` | prometheus.yml, grafana provisioning |
| Inter-container DNS | Container name = hostname | `kafka-1:9092`, `aerostream-producer:8090` |
| Env var injection | `environment:` + `.env` file | Kafka config, Grafana password, Spring profiles |
| Startup ordering | `depends_on: condition: service_healthy` | schema-registry, producer |
| Health check | `healthcheck: test:` | All Kafka brokers, schema-registry |
| Auto-restart | `restart: unless-stopped` | producer, kafka-ui |
| Command override | `command:` | prometheus flags |
| Conditional bean | `@Profile` in Spring | `TestController` not in production |
