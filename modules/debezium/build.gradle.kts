dependencies {
    compileOnly(libs.kafka.connect)
    compileOnly(libs.kafka.transforms)
    compileOnly(libs.debezium.api)
    // to be able to handle array types
    compileOnly(libs.postgres.driver)

    testImplementation(libs.testcontainers.core)
    testImplementation(testFixtures(project(":modules:fixtures:fixtures-kafka-connect")))
}