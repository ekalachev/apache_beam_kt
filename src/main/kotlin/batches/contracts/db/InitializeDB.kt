package batches.contracts.db

import utils.DatabaseUtils

fun createContractsDatabase() {
    val dbUrl = "jdbc:sqlite:contracts.db"

    // SQL commands to create the tables
    val createTables = """
        DROP TABLE IF EXISTS Contracts;
        DROP TABLE IF EXISTS ParentContracts;

        CREATE TABLE ParentContracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT
        );

        CREATE TABLE Contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER,
            contract_name TEXT,
            contract_value NUMERIC,
            start_date TEXT,
            end_date TEXT,
            FOREIGN KEY(parent_id) REFERENCES ParentContracts(id)
        );
    """

    val insertParentQuery = "INSERT INTO ParentContracts (name, description) VALUES (?, ?)"
    val insertContractQuery =
        "INSERT INTO Contracts (parent_id, contract_name, contract_value, start_date, end_date) VALUES (?, ?, ?, ?, ?)"

    // Example data for ParentContracts
    val parentContracts = listOf(
        listOf("Parent Contract A", "Description for Parent Contract A"),
        listOf("Parent Contract B", "Description for Parent Contract B"),
        listOf("Parent Contract C", "Description for Parent Contract C"),
        listOf("Parent Contract D", "Description for Parent Contract D"),
        listOf("Parent Contract E", "Description for Parent Contract E"),
        listOf("Parent Contract F", "Description for Parent Contract F"),
        listOf("Parent Contract G", "Description for Parent Contract G"),
        listOf("Parent Contract H", "Description for Parent Contract H"),
        listOf("Parent Contract I", "Description for Parent Contract I"),
        listOf("Parent Contract J", "Description for Parent Contract J"),
        listOf("Parent Contract K", "Description for Parent Contract K"),
        listOf("Parent Contract L", "Description for Parent Contract L"),
        listOf("Parent Contract M", "Description for Parent Contract M"),
        listOf("Parent Contract N", "Description for Parent Contract N"),
        listOf("Parent Contract O", "Description for Parent Contract O"),
        listOf("Parent Contract P", "Description for Parent Contract P"),
        listOf("Parent Contract Q", "Description for Parent Contract Q"),
        listOf("Parent Contract R", "Description for Parent Contract R"),
        listOf("Parent Contract S", "Description for Parent Contract S"),
        listOf("Parent Contract T", "Description for Parent Contract T")
    )

    // Example data for Contracts
    val contracts = listOf(
        listOf(1, "Contract 1A", 10000.0, "2023-01-01", "2024-01-01"),
        listOf(1, "Contract 2A", 15000.0, "2023-02-01", "2024-02-01"),
        listOf(2, "Contract 1B", 20000.0, "2023-03-01", "2024-03-01"),
        listOf(2, "Contract 2B", 25000.0, "2023-04-01", "2024-04-01"),
        listOf(3, "Contract 1C", 30000.0, "2023-05-01", "2024-05-01"),
        listOf(3, "Contract 2C", 35000.0, "2023-06-01", "2024-06-01"),
        listOf(4, "Contract 1D", 40000.0, "2023-07-01", "2024-07-01"),
        listOf(4, "Contract 2D", 45000.0, "2023-08-01", "2024-08-01"),
        listOf(5, "Contract 1E", 50000.0, "2023-09-01", "2024-09-01"),
        listOf(5, "Contract 2E", 55000.0, "2023-10-01", "2024-10-01"),
        listOf(6, "Contract 1F", 60000.0, "2023-11-01", "2024-11-01"),
        listOf(6, "Contract 2F", 65000.0, "2023-12-01", "2024-12-01"),
        listOf(7, "Contract 1G", 70000.0, "2023-01-01", "2024-01-01"),
        listOf(7, "Contract 2G", 75000.0, "2023-02-01", "2024-02-01"),
        listOf(8, "Contract 1H", 80000.0, "2023-03-01", "2024-03-01"),
        listOf(8, "Contract 2H", 85000.0, "2023-04-01", "2024-04-01"),
        listOf(9, "Contract 1I", 90000.0, "2023-05-01", "2024-05-01"),
        listOf(9, "Contract 2I", 95000.0, "2023-06-01", "2024-06-01"),
        listOf(10, "Contract 1J", 100000.0, "2023-07-01", "2024-07-01"),
        listOf(10, "Contract 2J", 105000.0, "2023-08-01", "2024-08-01"),
        listOf(11, "Contract 1K", 110000.0, "2023-09-01", "2024-09-01"),
        listOf(11, "Contract 2K", 115000.0, "2023-10-01", "2024-10-01"),
        listOf(12, "Contract 1L", 120000.0, "2023-11-01", "2024-11-01"),
        listOf(12, "Contract 2L", 125000.0, "2023-12-01", "2024-12-01"),
        listOf(13, "Contract 1M", 130000.0, "2023-01-01", "2024-01-01"),
        listOf(13, "Contract 2M", 135000.0, "2023-02-01", "2024-02-01"),
        listOf(14, "Contract 1N", 140000.0, "2023-03-01", "2024-03-01"),
        listOf(14, "Contract 2N", 145000.0, "2023-04-01", "2024-04-01"),
        listOf(15, "Contract 1O", 150000.0, "2023-05-01", "2024-05-01"),
        listOf(15, "Contract 2O", 155000.0, "2023-06-01", "2024-06-01"),
        listOf(16, "Contract 1P", 160000.0, "2023-07-01", "2024-07-01"),
        listOf(16, "Contract 2P", 165000.0, "2023-08-01", "2024-08-01"),
        listOf(17, "Contract 1Q", 170000.0, "2023-09-01", "2024-09-01"),
        listOf(17, "Contract 2Q", 175000.0, "2023-10-01", "2024-10-01"),
        listOf(18, "Contract 1R", 180000.0, "2023-11-01", "2024-11-01"),
        listOf(18, "Contract 2R", 185000.0, "2023-12-01", "2024-12-01"),
        listOf(19, "Contract 1S", 190000.0, "2023-01-01", "2024-01-01"),
        listOf(19, "Contract 2S", 195000.0, "2023-02-01", "2024-02-01"),
        listOf(20, "Contract 1T", 200000.0, "2023-03-01", "2024-03-01"),
        listOf(20, "Contract 2T", 205000.0, "2023-04-01", "2024-04-01"),
        listOf(1, "Contract 3A", 210000.0, "2023-05-01", "2024-05-01"),
        listOf(1, "Contract 4A", 215000.0, "2023-06-01", "2024-06-01"),
        listOf(2, "Contract 3B", 220000.0, "2023-07-01", "2024-07-01"),
        listOf(2, "Contract 4B", 225000.0, "2023-08-01", "2024-08-01"),
        listOf(3, "Contract 3C", 230000.0, "2023-09-01", "2024-09-01"),
        listOf(3, "Contract 4C", 235000.0, "2023-10-01", "2024-10-01"),
        listOf(4, "Contract 3D", 240000.0, "2023-11-01", "2024-11-01"),
        listOf(4, "Contract 4D", 245000.0, "2023-12-01", "2024-12-01"),
        listOf(5, "Contract 3E", 250000.0, "2023-01-01", "2024-01-01"),
        listOf(5, "Contract 4E", 255000.0, "2023-02-01", "2024-02-01"),
        listOf(6, "Contract 3F", 260000.0, "2023-03-01", "2024-03-01"),
        listOf(6, "Contract 4F", 265000.0, "2023-04-01", "2024-04-01"),
        listOf(7, "Contract 3G", 270000.0, "2023-05-01", "2024-05-01"),
        listOf(7, "Contract 4G", 275000.0, "2023-06-01", "2024-06-01"),
        listOf(8, "Contract 3H", 280000.0, "2023-07-01", "2024-07-01"),
        listOf(8, "Contract 4H", 285000.0, "2023-08-01", "2024-08-01"),
        listOf(9, "Contract 3I", 290000.0, "2023-09-01", "2024-09-01"),
        listOf(9, "Contract 4I", 295000.0, "2023-10-01", "2024-10-01"),
        listOf(10, "Contract 3J", 300000.0, "2023-11-01", "2024-11-01"),
        listOf(10, "Contract 4J", 305000.0, "2023-12-01", "2024-12-01"),
        listOf(11, "Contract 3K", 310000.0, "2023-01-01", "2024-01-01"),
        listOf(11, "Contract 4K", 315000.0, "2023-02-01", "2024-02-01"),
        listOf(12, "Contract 3L", 320000.0, "2023-03-01", "2024-03-01"),
        listOf(12, "Contract 4L", 325000.0, "2023-04-01", "2024-04-01"),
        listOf(13, "Contract 3M", 330000.0, "2023-05-01", "2024-05-01"),
        listOf(13, "Contract 4M", 335000.0, "2023-06-01", "2024-06-01"),
        listOf(14, "Contract 3N", 340000.0, "2023-07-01", "2024-07-01"),
        listOf(14, "Contract 4N", 345000.0, "2023-08-01", "2024-08-01"),
        listOf(15, "Contract 3O", 350000.0, "2023-09-01", "2024-09-01"),
        listOf(15, "Contract 4O", 355000.0, "2023-10-01", "2024-10-01"),
        listOf(16, "Contract 3P", 360000.0, "2023-11-01", "2024-11-01"),
        listOf(16, "Contract 4P", 365000.0, "2023-12-01", "2024-12-01"),
        listOf(17, "Contract 3Q", 370000.0, "2023-01-01", "2024-01-01"),
        listOf(17, "Contract 4Q", 375000.0, "2023-02-01", "2024-02-01"),
        listOf(18, "Contract 3R", 380000.0, "2023-03-01", "2024-03-01"),
        listOf(18, "Contract 4R", 385000.0, "2023-04-01", "2024-04-01"),
        listOf(19, "Contract 3S", 390000.0, "2023-05-01", "2024-05-01"),
        listOf(19, "Contract 4S", 395000.0, "2023-06-01", "2024-06-01"),
        listOf(20, "Contract 3T", 400000.0, "2023-07-01", "2024-07-01"),
        listOf(20, "Contract 4T", 405000.0, "2023-08-01", "2024-08-01")
    )

    // Create the tables
    DatabaseUtils.executeSqlUpdate(dbUrl, createTables)

    // Insert the ParentContracts data
    DatabaseUtils.executePreparedStatements(dbUrl, insertParentQuery, parentContracts)

    // Insert the Contracts data
    DatabaseUtils.executePreparedStatements(dbUrl, insertContractQuery, contracts)
}

fun testContractsQuery() {
    val dbUrl = "jdbc:sqlite:contracts.db"
    val query = """
        SELECT c.id, c.contract_name, c.contract_value, c.start_date, c.end_date, p.name AS parent_name
        FROM Contracts c
        JOIN ParentContracts p ON c.parent_id = p.id
    """

    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        println(
            "Contract ID: ${resultSet.getInt("id")}, Contract Name: ${resultSet.getString("contract_name")}, " +
                    "Contract Value: ${resultSet.getDouble("contract_value")}, Start Date: ${resultSet.getString("start_date")}, " +
                    "End Date: ${resultSet.getString("end_date")}, Parent Contract Name: ${resultSet.getString("parent_name")}"
        )
    }
}

fun main() {
    // Example of creating the database and inserting data
    createContractsDatabase()

    // Example of running a SELECT query
    testContractsQuery()
}