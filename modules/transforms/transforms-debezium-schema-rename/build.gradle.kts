dependencies {
    compileOnly(libs.kafka.connect)
    compileOnly(libs.kafka.transforms)
    compileOnly(libs.debezium.api)

    testImplementation(libs.testcontainers.core)
    testImplementation(testFixtures(project(":modules:fixtures:fixtures-kafka")))
    testImplementation(testFixtures(project(":modules:fixtures:fixtures-debezium")))
    testImplementation(testFixtures(project(":modules:fixtures:fixtures-postgres")))
}