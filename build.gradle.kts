val logbackVersion: String by project
val ktorVersion = "1.6.8"
val slf4jVersion = "2.0.13"

plugins {
    application
    kotlin("jvm") version "2.0.0"
    kotlin("plugin.serialization") version "2.1.0"
    id("com.adarshr.test-logger") version "4.0.0"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
}

group = "com.github.kjarosh.depldaprototype"
version = "0.1.0"
application {
    mainClass.set("com.github.davenury.ucac.ApplicationUcacKt")

    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
}

dependencies {

    implementation(project(":modules:common"))
    implementation(project(":modules:gmmf"))

    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")

    // ktor
    implementation("io.ktor:ktor-server:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-jackson:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-client-jackson:$ktorVersion")

    // object mapper
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.9.0")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.9.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.8.1")

    // metrics
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.0")
    implementation("io.ktor:ktor-metrics-micrometer:$ktorVersion")

    // traces
    implementation("io.jaegertracing:jaeger-client:1.8.1")
    implementation("com.zopa:ktor-opentracing:0.3.6")

    // k8s
    implementation("io.kubernetes:client-java:12.0.2")
    implementation("commons-cli:commons-cli:1.5.0")
    implementation("com.google.guava:guava:33.3.1-jre")

    implementation("com.github.loki4j:loki-logback-appender:1.5.1")
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("io.mockk:mockk:1.13.11")
    testImplementation("org.awaitility:awaitility:4.2.1")
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
    testImplementation("org.junit-pioneer:junit-pioneer:2.2.0")

    // for disabling AnsiConsole in tests
    testImplementation("org.fusesource.jansi:jansi:2.4.1")
}

tasks.test {
    useJUnitPlatform()
    maxParallelForks = 16
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.github.davenury.ucac.ApplicationKt"
    }
}

subprojects {
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "java")
    apply(plugin = "kotlin")

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    kotlin {
        jvmToolchain(11)
    }

    repositories {
        mavenCentral()
    }
}
