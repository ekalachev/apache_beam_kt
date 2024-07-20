plugins {
    kotlin("jvm") version "2.0.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    // Apache Beam dependencies
    implementation("org.apache.beam:beam-sdks-java-core:2.56.0")
    implementation("org.apache.beam:beam-runners-direct-java:2.56.0")
    implementation("org.apache.beam:beam-sdks-java-io-jdbc:2.56.0")

    // SQLite JDBC dependency
    implementation("org.xerial:sqlite-jdbc:3.41.2.2")

    // SLF4J and Logback dependencies
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("ch.qos.logback:logback-classic:1.4.12")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(11)
}