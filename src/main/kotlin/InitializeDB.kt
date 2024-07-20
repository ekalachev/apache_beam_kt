import utils.DatabaseUtils

fun createPresidentsDatabase() {
    val dbUrl = "jdbc:sqlite:presidents.db"

    // SQL commands to create the table and insert data
    val createTable = """
        DROP TABLE IF EXISTS presidents;
        CREATE TABLE presidents (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            second_name TEXT
        )
    """

    val insertQuery = "INSERT INTO presidents (first_name, second_name) VALUES (?, ?)"

    // Updated list of presidents with multiple entries for those who served multiple terms
    val presidents = listOf(
        listOf("George", "Washington"),
        listOf("John", "Adams"),
        listOf("Thomas", "Jefferson"),
        listOf("James", "Madison"),
        listOf("James", "Monroe"),
        listOf("John Quincy", "Adams"),
        listOf("Andrew", "Jackson"),
        listOf("Martin", "Van Buren"),
        listOf("William Henry", "Harrison"),
        listOf("John", "Tyler"),
        listOf("James K.", "Polk"),
        listOf("Zachary", "Taylor"),
        listOf("Millard", "Fillmore"),
        listOf("Franklin", "Pierce"),
        listOf("James", "Buchanan"),
        listOf("Abraham", "Lincoln"),
        listOf("Andrew", "Johnson"),
        listOf("Ulysses S.", "Grant"), // First term
        listOf("Ulysses S.", "Grant"), // Second term
        listOf("Rutherford B.", "Hayes"),
        listOf("James A.", "Garfield"),
        listOf("Chester A.", "Arthur"),
        listOf("Grover", "Cleveland"), // First term
        listOf("Benjamin", "Harrison"),
        listOf("Grover", "Cleveland"), // Second term (non-consecutive)
        listOf("William", "McKinley"), // First term
        listOf("William", "McKinley"), // Second term
        listOf("Theodore", "Roosevelt"),
        listOf("William H.", "Taft"),
        listOf("Woodrow", "Wilson"), // First term
        listOf("Woodrow", "Wilson"), // Second term
        listOf("Warren G.", "Harding"),
        listOf("Calvin", "Coolidge"),
        listOf("Herbert", "Hoover"),
        listOf("Franklin D.", "Roosevelt"), // First term
        listOf("Franklin D.", "Roosevelt"), // Second term
        listOf("Franklin D.", "Roosevelt"), // Third term
        listOf("Franklin D.", "Roosevelt"), // Fourth term
        listOf("Harry S.", "Truman"),
        listOf("Dwight D.", "Eisenhower"), // First term
        listOf("Dwight D.", "Eisenhower"), // Second term
        listOf("John F.", "Kennedy"),
        listOf("Lyndon B.", "Johnson"),
        listOf("Richard", "Nixon"),
        listOf("Gerald", "Ford"),
        listOf("Jimmy", "Carter"),
        listOf("Ronald", "Reagan"), // First term
        listOf("Ronald", "Reagan"), // Second term
        listOf("George H. W.", "Bush"),
        listOf("Bill", "Clinton"), // First term
        listOf("Bill", "Clinton"), // Second term
        listOf("George W.", "Bush"), // First term
        listOf("George W.", "Bush"), // Second term
        listOf("Barack", "Obama"), // First term
        listOf("Barack", "Obama"), // Second term
        listOf("Donald", "Trump"),
        listOf("Joe", "Biden")
    )

    // Example of running a DELETE query
    DatabaseUtils.executeSqlUpdate(dbUrl, "DELETE FROM presidents")
    DatabaseUtils.executePreparedStatements(dbUrl, insertQuery, presidents)
}

fun testPresidentsQuery() {
    val dbUrl = "jdbc:sqlite:presidents.db"
    val query = "SELECT * FROM presidents"

    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        println(
            "ID: ${resultSet.getInt("id")}, First Name: ${resultSet.getString("first_name")}, Second Name: ${resultSet.getString("second_name")}"
        )
    }
}

fun main() {
    // Example of creating the database and inserting data
    createPresidentsDatabase()

    // Example of running a SELECT query
    testPresidentsQuery()
}