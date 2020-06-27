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
			dependencies: ["CSQLite"]),
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
				.define("SQLITE_OMIT_DECLTYPE"),
				.define("SQLITE_OMIT_DEPRECATED"),
				.define("SQLITE_OMIT_PROGRESS_CALLBACK"),
				.define("SQLITE_OMIT_SHARED_CACHE"),
				.define("SQLITE_USE_ALLOCA", to: "1"),
				.define("SQLITE_OMIT_DEPRECATED"),
				.define("SQLITE_ENABLE_FTS5"),
				.define("SQLITE_ENABLE_RTREE"),
				.define("SQLITE_ENABLE_STAT4"),
				.define("SQLITE_ENABLE_SNAPSHOT"),
				.define("SQLITE_ENABLE_JSON1"),
				.define("SQLITE_EXTRA_INIT", to: "feisty_db_init"),
		]),
		.testTarget(
			name: "FeistyDBTests",
			dependencies: ["FeistyDB"]),
	],
	cLanguageStandard: .gnu11
)
