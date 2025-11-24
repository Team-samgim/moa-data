FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY build/libs/*.jar app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "app.jar"]
