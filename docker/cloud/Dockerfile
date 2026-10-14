FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY core core
COPY mock-standin mock-standin
COPY device device
COPY cloud cloud
COPY tools tools
RUN sh ./mvnw -q -B -DskipTests -pl cloud -am package

FROM eclipse-temurin:17-jre-jammy
RUN useradd --system --uid 10001 app
COPY --from=build /src/cloud/target/cloud-1.0.0-boot.jar /app/cloud.jar
USER app
ENV PORT=8080 \
    JAVA_OPTS="-Xmx320m -Xss512k -XX:MaxMetaspaceSize=128m -XX:+UseSerialGC -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/cloud.jar"]
