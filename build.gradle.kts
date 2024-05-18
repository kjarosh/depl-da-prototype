val logbackVersion: String by project
val ktorVersion = "1.6.8"
val ratisVersion = "2.2.0"
val slf4jVersion = "2.0.12"

plugins {
    application
    kotlin("jvm") version "1.9.24"
    kotlin("plugin.serialization") version "1.9.24"
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
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.1")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.5")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.7.5")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.8.1")

    // metrics
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.0")
    implementation("io.ktor:ktor-metrics-micrometer:$ktorVersion")

    // traces
    implementation("io.jaegertracing:jaeger-client:1.8.1")
    implementation("com.zopa:ktor-opentracing:0.3.6")

    implementation("org.apache.ratis:ratis:$ratisVersion")
    implementation("org.apache.ratis:ratis-proto:$ratisVersion")
    implementation("org.apache.ratis:ratis-grpc:$ratisVersion")
    implementation("org.apache.ratis:ratis-common:$ratisVersion")
    implementation("org.apache.ratis:ratis-server-api:$ratisVersion")
    implementation("org.apache.ratis:ratis-tools:$ratisVersion")
    implementation("org.apache.ratis:ratis-client:$ratisVersion")
    implementation("org.apache.ratis:ratis-thirdparty-misc:1.0.5")

    implementation("com.github.loki4j:loki-logback-appender:1.5.1")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("io.mockk:mockk:1.13.11")
    testImplementation("org.awaitility:awaitility:4.2.1")
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:junit-jupiter:1.19.7")
    testImplementation("org.junit-pioneer:junit-pioneer:2.2.0")

    // for disabling AnsiConsole in tests
    testImplementation("org.fusesource.jansi:jansi:2.4.1")
}

tasks.withType<JavaCompile>().configureEach {
    targetCompatibility = "11"
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

    repositories {
        mavenCentral()
    }

    tasks.withType<JavaCompile>().configureEach {
        targetCompatibility = "11"
    }
}
