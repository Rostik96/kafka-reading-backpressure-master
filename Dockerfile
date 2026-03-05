ARG base_image=eclipse-temurin:21-jre
ARG build_image=maven:3.9.8-eclipse-temurin-21
ARG VERSION=1.0.0-SNAPSHOT

FROM ${build_image} AS build

WORKDIR /workspace
COPY . .

ARG MODULE
RUN mvn -q -DskipTests -pl ${MODULE} -am package

FROM ${base_image} AS builder

ARG MODULE
ARG VERSION=1.0.0-SNAPSHOT
COPY --from=build /workspace/${MODULE}/target/${MODULE}-${VERSION}.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher

FROM ${base_image}

WORKDIR /data/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder app/dependencies ./
COPY --from=builder app/spring-boot-loader ./
COPY --from=builder app/snapshot-dependencies ./
COPY --from=builder app/application ./

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
