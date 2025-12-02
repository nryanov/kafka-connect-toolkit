import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar.Companion.shadowJar

plugins {
    id("java")
    id("com.gradleup.shadow") version "9.2.2" apply false
}

allprojects {
    group = "com.nryanov.kafka.connect.toolkit"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
        mavenLocal()
        maven {
            url = uri("https://packages.confluent.io/maven")
        }
    }
}

subprojects {
    apply(plugin="java")
    apply(plugin="java-library")
    apply(plugin="com.gradleup.shadow")

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
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

        dependsOn(tasks.shadowJar)
    }

    tasks.shadowJar {
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        filesMatching("META-INF/services/**") {
            duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }

        archiveFileName = "${project.name}.jar"
    }
}