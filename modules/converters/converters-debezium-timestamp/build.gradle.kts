dependencies {
    compileOnly(libs.kafka.connect)
    compileOnly(libs.debezium.api)

    testImplementation(testFixtures(project(":modules:fixtures:fixtures-debezium")))
}