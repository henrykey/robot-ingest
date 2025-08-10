# ---- Build stage ----
FROM gradle:8.9.0-jdk21 AS build
WORKDIR /src
COPY . /src/
RUN gradle :writer-service:bootJar --no-daemon

# ---- Run stage ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/writer-service/build/libs/writer-service-0.1.0.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar","--config=/app/config.yml"]
