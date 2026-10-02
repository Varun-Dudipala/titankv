# syntax=docker/dockerfile:1
# Build stage: compile and package the server jar
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre
RUN useradd --system --create-home titankv
WORKDIR /opt/titankv
COPY --from=build /src/target/titankv-1.0.0.jar titankv.jar
RUN mkdir -p /var/lib/titankv logs && chown -R titankv /var/lib/titankv /opt/titankv
USER titankv
ENV TITANKV_DATA_DIR=/var/lib/titankv
# client TCP, metrics HTTP, gossip UDP (port + 1000)
EXPOSE 9001 9091 10001/udp
ENTRYPOINT ["java", "-jar", "titankv.jar"]
CMD ["--port", "9001"]
