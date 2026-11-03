plugins {
    `java-library`
}

group = "io.github.agampal007"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Compile for Java 21 bytecode so the build works on any JDK from 21 up.
tasks.withType<JavaCompile>().configureEach {
    options.release = 21
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.register<JavaExec>("sweep") {
    description = "Recall and speed of HNSW against exact search on random vectors."
    group = "verification"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "io.github.agampal007.vectordb.RandomDataSweep"
}

tasks.register<JavaExec>("sift") {
    description = "HNSW against exact search on SIFT-1M (run scripts/download_sift.sh first)."
    group = "verification"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "io.github.agampal007.vectordb.SiftBenchmark"
    maxHeapSize = "8g"
    args = listOf(
        "data/sift",
        (findProperty("n") ?: "1000000").toString(),
        (findProperty("m") ?: "16").toString(),
        (findProperty("efConstruction") ?: "200").toString(),
    )
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
