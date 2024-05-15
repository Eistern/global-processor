plugins {
    id("java")
}

group = "org.eistern"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("org.slf4j:slf4j-api:2.0.13")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.25.1")
    testImplementation("ch.qos.logback:logback-classic:1.5.6")
}

tasks.test {
    useJUnitPlatform()
}