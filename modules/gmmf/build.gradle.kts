plugins {
    id("application")
}

sourceSets {
    main {
        java.srcDir("src/main/java")
    }
}

dependencies {
    // GMMF
    compileOnly("org.projectlombok:lombok:1.18.40")
    annotationProcessor("org.projectlombok:lombok:1.18.40")
    testCompileOnly("org.projectlombok:lombok:1.18.40")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.40")
    implementation("com.github.javafaker:javafaker:1.0.2")
    implementation("com.google.guava:guava:33.3.1-jre")
    implementation(platform("org.springframework.boot:spring-boot-dependencies:2.7.18"))
    implementation("org.springframework.boot:spring-boot-starter:2.7.18")
    implementation("org.springframework.boot:spring-boot-starter-web:2.7.18")
    implementation("org.springframework:spring-core:5.3.23")
    implementation("org.yaml:snakeyaml:1.33")
    implementation("io.dropwizard.metrics:metrics-core:4.2.12")
    implementation("org.apache.commons:commons-text:1.10.0")
}

repositories {
    mavenCentral()
}
