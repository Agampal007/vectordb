# Build: compile and assemble build/install/vectordb (a start script and the jar).
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
# Downloads Gradle in its own layer, so editing the source does not download it again.
RUN ./gradlew --no-daemon --version
COPY src src
RUN ./gradlew --no-daemon installDist

# Run: a JRE and the assembled server, as an unprivileged user.
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --no-create-home vectordb \
    && mkdir /data \
    && chown vectordb /data
COPY --from=build /src/build/install/vectordb /opt/vectordb
USER vectordb
VOLUME /data
EXPOSE 8080
# The start script ends in "exec java", so "docker stop" reaches the JVM, which snapshots and exits.
ENTRYPOINT ["/opt/vectordb/bin/vectordb", "--host", "0.0.0.0", "--data", "/data"]
