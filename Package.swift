// swift-tools-version:5.6
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

// For pre-update hook support:
//  - Uncomment lines containing `SQLITE_ENABLE_PREUPDATE_HOOK`
//
// For session support:
//  - Uncomment lines containing `SQLITE_ENABLE_PREUPDATE_HOOK`
//  - Uncomment lines containing `SQLITE_ENABLE_SESSION`

let package = Package(
	name: "FeistyDB",
	products: [
		// Products define the executables and libraries produced by a package, and make them visible to other packages.
		.library(
			name: "FeistyDB",
			targets: [
				"CFeistyDB",
				"FeistyDB",
			]),
	],
	dependencies: [
		// Dependencies declare other packages that this package depends on.
		// .package(url: /* package url */, from: "1.0.0"),
		.package(url: "https://github.com/sbooth/CSQLite", from: "3.51.0")
	],
	targets: [
		// Targets are the basic building blocks of a package. A target can define a module or a test suite.
		// Targets can depend on other targets in this package, and on products in packages which this package depends on.
		.target(
			name: "CFeistyDB",
			dependencies: [
				.product(name: "CSQLite", package: "CSQLite"),
			]
		),
		.target(
			name: "FeistyDB",
			dependencies: [
				"CFeistyDB",
			]
//			swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
//			]
		),
		.testTarget(
			name: "FeistyDBTests",
			dependencies: [
				"FeistyDB",
			]
//			, swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
//			]
		),
		.testTarget(
			name: "FeistyDBPerformanceTests",
			dependencies: [
				"FeistyDB",
			]),
	],
	cLanguageStandard: .gnu11
)
