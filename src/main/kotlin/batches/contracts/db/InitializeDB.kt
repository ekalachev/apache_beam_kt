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
    val insertContractQuery = "INSERT INTO Contracts (parent_id, contract_name, contract_value, start_date, end_date) VALUES (?, ?, ?, ?, ?)"

    // Example data for ParentContracts
    val parentContracts = listOf(
        listOf("Parent Contract A", "Description for Parent Contract A"),
        listOf("Parent Contract B", "Description for Parent Contract B")
    )

    // Example data for Contracts
    val contracts = listOf(
        listOf(1, "Contract 1A", 10000.0, "2023-01-01", "2024-01-01"),
        listOf(1, "Contract 2A", 15000.0, "2023-02-01", "2024-02-01"),
        listOf(2, "Contract 1B", 20000.0, "2023-03-01", "2024-03-01"),
        listOf(2, "Contract 2B", 25000.0, "2023-04-01", "2024-04-01")
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