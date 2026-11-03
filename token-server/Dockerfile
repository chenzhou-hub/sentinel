FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai \
    JAVA_OPTS="-Xms512m -Xmx512m -XX:+UseG1GC"
COPY --from=builder /build/target/token-server-1.0.0.jar app.jar
# 18730 Token Server 监听端口 / 18731 管理端口
EXPOSE 18730 18731
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
