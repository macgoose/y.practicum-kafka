# Собрать исполняемый JAR с зависимостями на JDK 17.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Собрать приложение без запуска тестов.
RUN mvn -B -DskipTests package

# Оставить в итоговом образе только Java Runtime и собранное приложение.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/kafka-app.jar app.jar
# Запустить общую точку входа Application из манифеста JAR.
ENTRYPOINT ["java", "-jar", "app.jar"]
