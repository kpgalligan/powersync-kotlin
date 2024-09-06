import java.util.Properties
import com.codingfeline.buildkonfig.compiler.FieldSpec.Type.STRING

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    alias(libs.plugins.jetbrainsCompose)
    alias(libs.plugins.cocoapods)
    alias(libs.plugins.buildKonfig)
}

version = "1.0-SNAPSHOT"

kotlin {
    androidTarget()

//    jvm()
    iosX64()
    iosArm64()
    iosSimulatorArm64()

    cocoapods {
        version = "1.0.0"
        summary = "A shared library for TodoAppLite PowerSync app"
        homepage = "none"
        ios.deploymentTarget = "14.1"
        podfile = project.file("../iosApp/Podfile")
        pod("powersync-sqlite-core") {
            linkOnly = true
        }

        framework {
            baseName = "shared"
            isStatic = true
            export("com.powersync:core")
        }
    }
    sourceSets {
        commonMain.dependencies {
            api("com.powersync:core")
            api("com.powersync:connector-supabase")
            implementation(libs.uuid)
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.components.resources)
            implementation(compose.materialIconsExtended)
        }
        androidMain.dependencies {
            api(libs.androidx.activity.compose)
            api(libs.androidx.appcompat)
            api(libs.androidx.core)
        }

//        jvmMain.dependencies {
//            implementation(compose.desktop.common)
//        }
    }
}

android {
    namespace = "com.powersync.demos"
    compileSdk = libs.versions.android.compileSdk.get().toInt()
    sourceSets["main"].manifest.srcFile("src/androidMain/AndroidManifest.xml")
    sourceSets["main"].res.srcDirs("src/androidMain/res")
    sourceSets["main"].resources.srcDirs("src/commonMain/resources")

    defaultConfig {
        minSdk = libs.versions.android.minSdk.get().toInt()
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlin {
        jvmToolchain(libs.versions.java.get().toInt())
    }
}

val localProperties = Properties().apply {
    try {
        load(rootProject.file("local.properties").reader())
    } catch (ignored: java.io.IOException) {
        throw Error("local.properties file not found")
    }
}

buildkonfig {
    packageName = "com.powersync.demos"
    objectName = "Config"

    defaultConfigs {
        fun stringConfigField(name: String) {
            val propValue = localProperties.getProperty(name, "")
            if (propValue.isBlank()) {
                println("Warning: Property $name not found in local.properties")
            } else {
                buildConfigField(STRING, name, propValue)
            }
        }

        stringConfigField("POWERSYNC_URL")
        stringConfigField("SUPABASE_URL")
        stringConfigField("SUPABASE_ANON_KEY")
    }
}
