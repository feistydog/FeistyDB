// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
	name: "FeistyDB",
	platforms: [
		.macOS(.v10_12),
	],
	products: [
		// Products define the executables and libraries produced by a package, and make them visible to other packages.
		.library(
			name: "FeistyDB",
			targets: ["FeistyDB"]),
		.library(
			name: "CSQLite",
			targets: ["CSQLite"]),
	],
	dependencies: [
		// Dependencies declare other packages that this package depends on.
		// .package(url: /* package url */, from: "1.0.0"),
	],
	targets: [
		// Targets are the basic building blocks of a package. A target can define a module or a test suite.
		// Targets can depend on other targets in this package, and on products in packages which this package depends on.
		.target(
			name: "FeistyDB",
			dependencies: ["CSQLite"],
			cSettings: [
				// If pre-update hook support is desired uncomment the following line
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK", to: "1"),
			],
			swiftSettings: [
				// If pre-update hook support is desired uncomment the following line
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK")
		]),
		.target(
			name: "CSQLite",
			dependencies: [],
			cSettings: [
				.define("SQLITE_DQS", to: "0"),
				.define("SQLITE_THREADSAFE", to: "0"),
				.define("SQLITE_DEFAULT_MEMSTATUS", to: "0"),
				.define("SQLITE_DEFAULT_WAL_SYNCHRONOUS", to: "1"),
				.define("SQLITE_LIKE_DOESNT_MATCH_BLOBS"),
				.define("SQLITE_MAX_EXPR_DEPTH", to: "0"),
				.define("SQLITE_OMIT_DECLTYPE", to: "1"),
				.define("SQLITE_OMIT_DEPRECATED", to: "1"),
				.define("SQLITE_OMIT_PROGRESS_CALLBACK", to: "1"),
				.define("SQLITE_OMIT_SHARED_CACHE", to: "1"),
				.define("SQLITE_USE_ALLOCA", to: "1"),
				.define("SQLITE_OMIT_DEPRECATED", to: "1"),
				// If pre-update hook support is desired uncomment the following line
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK", to: "1"),
				.define("SQLITE_ENABLE_FTS5", to: "1"),
				.define("SQLITE_ENABLE_RTREE", to: "1"),
				.define("SQLITE_ENABLE_STAT4", to: "1"),
				.define("SQLITE_ENABLE_SNAPSHOT", to: "1"),
				.define("SQLITE_ENABLE_JSON1", to: "1"),
				.define("SQLITE_EXTRA_INIT", to: "feisty_db_init"),
		]),
		.testTarget(
			name: "FeistyDBTests",
			dependencies: ["FeistyDB"]),
	],
	cLanguageStandard: .gnu11
)
