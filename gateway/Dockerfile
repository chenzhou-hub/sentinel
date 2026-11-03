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
COPY --from=builder /build/target/gateway-1.0.0.jar app.jar
# 8082 网关端口 / 18719 Sentinel 命令端口（Dashboard 下发规则用）
EXPOSE 8082 18719
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
