package batches.presidents.db

import utils.DatabaseUtils

fun createPresidentsDatabase() {
    val dbUrl = "jdbc:sqlite:presidents.db"

    // SQL commands to create the table and insert data
    val createTable = """
        DROP TABLE IF EXISTS presidents;
        CREATE TABLE presidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            second_name TEXT,
            start_year INTEGER,
            end_year INTEGER
        )
    """

    val insertQuery = "INSERT INTO presidents (first_name, second_name, start_year, end_year) VALUES (?, ?, ?, ?)"

    // Updated list of presidents with accurate terms
    val presidents = listOf(
        listOf("George", "Washington", 1789, 1797),
        listOf("John", "Adams", 1797, 1801),
        listOf("Thomas", "Jefferson", 1801, 1809),
        listOf("James", "Madison", 1809, 1817),
        listOf("James", "Monroe", 1817, 1825),
        listOf("John Quincy", "Adams", 1825, 1829),
        listOf("Andrew", "Jackson", 1829, 1837),
        listOf("Martin", "Van Buren", 1837, 1841),
        listOf("William Henry", "Harrison", 1841, 1841),
        listOf("John", "Tyler", 1841, 1845),
        listOf("James K.", "Polk", 1845, 1849),
        listOf("Zachary", "Taylor", 1849, 1850),
        listOf("Millard", "Fillmore", 1850, 1853),
        listOf("Franklin", "Pierce", 1853, 1857),
        listOf("James", "Buchanan", 1857, 1861),
        listOf("Abraham", "Lincoln", 1861, 1865),
        listOf("Andrew", "Johnson", 1865, 1869),
        listOf("Ulysses S.", "Grant", 1869, 1873), // First term
        listOf("Ulysses S.", "Grant", 1873, 1877), // Second term
        listOf("Rutherford B.", "Hayes", 1877, 1881),
        listOf("James A.", "Garfield", 1881, 1881),
        listOf("Chester A.", "Arthur", 1881, 1885),
        listOf("Grover", "Cleveland", 1885, 1889), // First term
        listOf("Benjamin", "Harrison", 1889, 1893),
        listOf("Grover", "Cleveland", 1893, 1897), // Second term (non-consecutive)
        listOf("William", "McKinley", 1897, 1901), // First term
        listOf("William", "McKinley", 1901, 1901), // Second term (until assassination)
        listOf("Theodore", "Roosevelt", 1901, 1905), // First term
        listOf("Theodore", "Roosevelt", 1905, 1909), // Second term
        listOf("William H.", "Taft", 1909, 1913),
        listOf("Woodrow", "Wilson", 1913, 1917), // First term
        listOf("Woodrow", "Wilson", 1917, 1921), // Second term
        listOf("Warren G.", "Harding", 1921, 1923),
        listOf("Calvin", "Coolidge", 1923, 1925), // First term (completing Harding's term)
        listOf("Calvin", "Coolidge", 1925, 1929), // Second term
        listOf("Herbert", "Hoover", 1929, 1933),
        listOf("Franklin D.", "Roosevelt", 1933, 1937), // First term
        listOf("Franklin D.", "Roosevelt", 1937, 1941), // Second term
        listOf("Franklin D.", "Roosevelt", 1941, 1945), // Third term
        listOf("Franklin D.", "Roosevelt", 1945, 1945), // Fourth term
        listOf("Harry S.", "Truman", 1945, 1949), // First term
        listOf("Harry S.", "Truman", 1949, 1953), // Second term
        listOf("Dwight D.", "Eisenhower", 1953, 1957), // First term
        listOf("Dwight D.", "Eisenhower", 1957, 1961), // Second term
        listOf("John F.", "Kennedy", 1961, 1963),
        listOf("Lyndon B.", "Johnson", 1963, 1965), // First term (completing Kennedy's term)
        listOf("Lyndon B.", "Johnson", 1965, 1969), // Second term
        listOf("Richard", "Nixon", 1969, 1973), // First term
        listOf("Richard", "Nixon", 1973, 1974), // Second term (until resignation)
        listOf("Gerald", "Ford", 1974, 1977),
        listOf("Jimmy", "Carter", 1977, 1981),
        listOf("Ronald", "Reagan", 1981, 1985), // First term
        listOf("Ronald", "Reagan", 1985, 1989), // Second term
        listOf("George H. W.", "Bush", 1989, 1993),
        listOf("Bill", "Clinton", 1993, 1997), // First term
        listOf("Bill", "Clinton", 1997, 2001), // Second term
        listOf("George W.", "Bush", 2001, 2005), // First term
        listOf("George W.", "Bush", 2005, 2009), // Second term
        listOf("Barack", "Obama", 2009, 2013), // First term
        listOf("Barack", "Obama", 2013, 2017), // Second term
        listOf("Donald", "Trump", 2017, 2021),
        listOf("Joe", "Biden", 2021, 2025)
    )

    // Create the table
    DatabaseUtils.executeSqlUpdate(dbUrl, createTable)

    // Insert the presidents data
    DatabaseUtils.executePreparedStatements(dbUrl, insertQuery, presidents)
}

fun testPresidentsQuery() {
    val dbUrl = "jdbc:sqlite:presidents.db"
    val query = "SELECT * FROM presidents"

    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        println(
            "ID: ${resultSet.getInt("id")}, First Name: ${resultSet.getString("first_name")}, Second Name: ${resultSet.getString("second_name")}, Start Year: ${resultSet.getInt("start_year")}, End Year: ${resultSet.getInt("end_year")}"
        )
    }
}

fun main() {
    // Example of creating the database and inserting data
    createPresidentsDatabase()

    // Example of running a SELECT query
    testPresidentsQuery()
}