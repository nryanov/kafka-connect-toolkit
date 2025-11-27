plugins {
    id("java")
}

allprojects {
    group = "com.nryanov.kafka.connect.toolkit"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
        mavenLocal()
    }
}

subprojects {
    apply(plugin="java")
    apply(plugin="java-library")

    java {
        sourceCompatibility = JavaVersion.VERSION_24
        targetCompatibility = JavaVersion.VERSION_24
    }

    val libs = rootProject.libs

    dependencies {
        testImplementation(libs.slf4j)
        testImplementation(libs.logback)
        testImplementation(platform(libs.junit.bom))
        testImplementation(libs.junit)
        testRuntimeOnly(libs.junit.platform.launcher)
    }

    tasks.test {
        useJUnitPlatform()
    }
}