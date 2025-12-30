dependencies {
    implementation(libs.google.guava)

    implementation(project(":modules:core"))
    compileOnly(libs.kafka.connect)
    compileOnly(libs.kafka.transforms)

    testImplementation(libs.kafka.connect)
    testImplementation(libs.kafka.transforms)
}