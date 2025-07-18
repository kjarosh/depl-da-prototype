import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktorVersion = "3.2.1"
val logbackVersion: String by project

plugins {
    id("application")
    kotlin("jvm")
}

application {
    mainClass.set("com.github.davenury.tests.TestNotificationServiceKt")
}

sourceSets {
    main {
        java.srcDir("src/main/kotlin")
    }
}
dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    implementation("io.ktor:ktor-server:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")

    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-client-jackson:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("com.github.loki4j:loki-logback-appender:1.4.0")

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.2")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")
    implementation("io.prometheus:simpleclient_pushgateway:0.16.0")

    implementation("com.sksamuel.hoplite:hoplite-core:2.9.0")

    implementation(project(":modules:common"))

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("io.mockk:mockk:1.13.16")
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.compilerOptions.jvmTarget.set(JvmTarget.JVM_11)
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.compilerOptions.jvmTarget.set(JvmTarget.JVM_11)

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.github.davenury.tests.TestNotificationServiceKt"
    }
}
