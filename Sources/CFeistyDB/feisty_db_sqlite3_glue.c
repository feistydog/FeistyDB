
// Workaround the Swift Package Manager requirement to have at least one
// source file per package
// See https://forums.swift.org/t/header-only-library-using-swift-package-manager/42700

static void feisty_db_initialize(void) __attribute__ ((constructor));
static void feisty_db_initialize()
{
}
