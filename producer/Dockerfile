# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-amazoncorretto-21 AS builder
WORKDIR /build

# Cache dependencies separately from source code
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM amazoncorretto:21-alpine
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /build/target/aerostream-producer-*.jar app.jar

USER appuser
EXPOSE 8090

ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "app.jar"]
