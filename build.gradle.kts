val logbackVersion: String by project
val ktorVersion = "3.2.1"
val slf4jVersion = "2.0.17"

plugins {
    application
    kotlin("jvm") version "2.2.0"
    kotlin("plugin.serialization") version "2.1.21"
    id("com.adarshr.test-logger") version "4.0.0"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2"
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
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-client-jackson:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")

    // object mapper
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.9.0")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.9.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-debug:1.10.2")

    // metrics
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.0")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")
    // k8s
    implementation("io.kubernetes:client-java:12.0.2")
    implementation("commons-cli:commons-cli:1.5.0")
    implementation("com.google.guava:guava:33.3.1-jre")

    implementation("com.github.loki4j:loki-logback-appender:1.5.1")
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("io.mockk:mockk:1.13.16")
    testImplementation("org.awaitility:awaitility:4.3.0")
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

    repositories {
        mavenCentral()
    }
}

tasks.register<JavaExec>("runK8sTests") {
    group = "K8s Tests"

    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.github.davenury.ucac.gmmf.tests.KubernetesClient")

    args(
        "--namespace",
        "kjarosz",
        "--peersets",
        "2,2,3",
        "--set-up-peers",
        "--constant-load-opts",
        "-l -n 1",
        "--graph",
        "graphs/graph-small-3ps.json",
    )
}

tasks.register<JavaExec>("pushGraphsToPvc") {
    group = "K8s Tests"

    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.github.davenury.ucac.gmmf.tests.PushDirectoryToPvcClient")

    args(
        "--namespace",
        "kjarosz",
        "--dir",
        "./graphs",
        "--pvc",
        "constant-client-graph",
    )
}

tasks.register<Exec>("buildImage") {
    group = "K8s Tests"

    dependsOn("assemble")

    commandLine("docker", "build", ".", "-t", "ghcr.io/kjarosh/depl-da-prototype:dev")
}

tasks.register<Exec>("pushImage") {
    group = "K8s Tests"

    dependsOn("buildImage")

    commandLine("docker", "push", "ghcr.io/kjarosh/depl-da-prototype:dev")
}
