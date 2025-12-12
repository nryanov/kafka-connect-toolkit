dependencies {
    implementation(libs.google.guava)

    compileOnly(libs.kafka.connect)
    compileOnly(libs.kafka.transforms)

    testImplementation(libs.kafka.connect)
    testImplementation(libs.kafka.transforms)
}