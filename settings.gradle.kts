pluginManagement {
    repositories {
        google()
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://maven.pkg.jetbrains.space/public/p/compose/dev")
        maven("https://jitpack.io")
    }
}


rootProject.name = "powersync"

include(":core")
include(":connectors")
include(":dialect")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
