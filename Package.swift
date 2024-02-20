// swift-tools-version:5.3
import PackageDescription

let packageName = "powersync-swift"

let package = Package(
    name: packageName,
    platforms: [
        .iOS(.v13)
    ],
    products: [
        .library(
            name: packageName,
            targets: [packageName]
        ),
    ],
    targets: [
        .binaryTarget(
            name: packageName,
            path: "./powersync-swift/build/XCFrameworks/debug/\(packageName).xcframework"
        )
        ,
    ]
)