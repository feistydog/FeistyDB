// swift-tools-version:5.2
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
			dependencies: ["CSQLite"]
//			, cSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK", to: "1"),
//				.define("SQLITE_ENABLE_SESSION", to: "1"),
//			],
//			swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
//			]
		),
		.target(
			name: "CSQLite",
			dependencies: [],
			cSettings: [
				// from config.h
				.define("HAVE_DLFCN_H", to: "1"),
				.define("HAVE_FDATASYNC", to: "1"),
				.define("HAVE_GMTIME_R", to: "1"),
				.define("HAVE_INT16_T", to: "1"),
				.define("HAVE_INT32_T", to: "1"),
				.define("HAVE_INT64_T", to: "1"),
				.define("HAVE_INT8_T", to: "1"),
				.define("HAVE_INTPTR_T", to: "1"),
				.define("HAVE_INTTYPES_H", to: "1"),
				.define("HAVE_ISNAN", to: "1"),
				.define("HAVE_LOCALTIME_R", to: "1"),
				.define("HAVE_MEMORY_H", to: "1"),
				.define("HAVE_PREAD", to: "1"),
				.define("HAVE_PWRITE", to: "1"),
				.define("HAVE_STDINT_H", to: "1"),
				.define("HAVE_STDLIB_H", to: "1"),
				.define("HAVE_STRINGS_H", to: "1"),
				.define("HAVE_STRING_H", to: "1"),
				.define("HAVE_SYS_STAT_H", to: "1"),
				.define("HAVE_SYS_TYPES_H", to: "1"),
				.define("HAVE_UINT16_T", to: "1"),
				.define("HAVE_UINT32_T", to: "1"),
				.define("HAVE_UINT64_T", to: "1"),
				.define("HAVE_UINT8_T", to: "1"),
				.define("HAVE_UINTPTR_T", to: "1"),
				.define("HAVE_UNISTD_H", to: "1"),
				.define("HAVE_USLEEP", to: "1"),
				.define("HAVE_UTIME", to: "1"),
				.define("STDC_HEADERS", to: "1"),
				// SQLite capabilities
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
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK", to: "1"),
//				.define("SQLITE_ENABLE_SESSION", to: "1"),
				.define("SQLITE_ENABLE_FTS5", to: "1"),
				.define("SQLITE_ENABLE_RTREE", to: "1"),
				.define("SQLITE_ENABLE_STAT4", to: "1"),
				.define("SQLITE_ENABLE_SNAPSHOT", to: "1"),
				.define("SQLITE_ENABLE_JSON1", to: "1"),
				.define("SQLITE_ENABLE_MATH_FUNCTIONS", to: "1"),
				.define("SQLITE_EXTRA_INIT", to: "feisty_db_init"),
		],
		linkerSettings: [
			.linkedLibrary("m")
		]),
		.testTarget(
			name: "FeistyDBTests",
			dependencies: ["FeistyDB"]
//			, swiftSettings: [
//				.define("SQLITE_ENABLE_PREUPDATE_HOOK"),
//				.define("SQLITE_ENABLE_SESSION"),
//			]
		),
		.testTarget(
			name: "CSQLitePerformanceTests",
			dependencies: ["CSQLite"]),
		.testTarget(
			name: "FeistyDBPerformanceTests",
			dependencies: ["FeistyDB"]),
	],
	cLanguageStandard: .gnu11
)
