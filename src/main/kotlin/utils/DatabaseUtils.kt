package utils

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

object DatabaseUtils {
    fun executeSqlQuery(dbUrl: String, query: String, processRow: (ResultSet) -> Unit) {
        try {
            val connection: Connection = DriverManager.getConnection(dbUrl)
            val statement = connection.createStatement()
            val resultSet: ResultSet = statement.executeQuery(query)
            while (resultSet.next()) {
                processRow(resultSet)
            }
            resultSet.close()
            statement.close()
            connection.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun executeSqlUpdate(dbUrl: String, vararg updates: String) {
        try {
            val connection: Connection = DriverManager.getConnection(dbUrl)
            val statement = connection.createStatement()
            for (update in updates) {
                statement.executeUpdate(update)
            }
            statement.close()
            connection.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun executePreparedStatements(dbUrl: String, query: String, parameters: List<List<Any>>) {
        try {
            val connection: Connection = DriverManager.getConnection(dbUrl)
            val preparedStatement = connection.prepareStatement(query)
            for (params in parameters) {
                for ((index, param) in params.withIndex()) {
                    when (param) {
                        is String -> preparedStatement.setString(index + 1, param)
                        is Int -> preparedStatement.setInt(index + 1, param)
                        else -> throw IllegalArgumentException("Unsupported parameter type")
                    }
                }
                preparedStatement.executeUpdate()
            }
            preparedStatement.close()
            connection.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}