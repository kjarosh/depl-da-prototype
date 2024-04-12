import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktorVersion = "1.6.8"

plugins {
    id("application")
    kotlin("jvm")
}

sourceSets {
    main {
        java.srcDir("src/main/kotlin")
    }
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")

    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-client-jackson:$ktorVersion")

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.2")
    // traces
    implementation("io.jaegertracing:jaeger-client:1.8.1")
    implementation("com.zopa:ktor-opentracing:0.3.6")

    implementation("io.ktor:ktor-metrics-micrometer:$ktorVersion")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.5")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.7.5")

    implementation(kotlin("stdlib-jdk8"))

    implementation("redis.clients:jedis:5.1.2")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:junit-jupiter:1.19.7")
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}
tasks.test {
    useJUnitPlatform()
}
