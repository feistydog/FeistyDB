//: # FeistyDB
//: A powerful and performant Swift interface to [SQLite](https://sqlite.org).
import FeistyDB
//: Create an in-memory database for use on a single thread or queue
let db = try Database()
//: Create a table
try db.execute(sql: "create table t1(a, b);")
//: Insert a row
try db.execute(sql: "insert into t1(a, b) values (?, ?);", parameterValues: 33, "lulu")
//: Retrieve the values
try db.execute(sql: "select a, b from t1;") { row in
	let a: Int = try row.value(at: 0)
	let b: String = try row.value(at: 1)
}