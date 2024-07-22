package batches.contracts

import batches.contracts.coders.ContractCoder
import batches.contracts.coders.ParentContractCoder
import batches.contracts.model.Contract
import batches.contracts.model.ParentContract
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.MetricNameFilter
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors
import utils.DatabaseUtils

// Helper function to get all Contracts
fun getAllContracts(dbUrl: String): List<Contract> {
    val contracts = mutableListOf<Contract>()
    val query = "SELECT id, parent_id, contract_name, contract_value, start_date, end_date FROM Contracts"

    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        val contract = Contract(
            id = resultSet.getInt("id"),
            parentId = resultSet.getInt("parent_id"),
            contractName = resultSet.getString("contract_name"),
            contractValue = resultSet.getDouble("contract_value"),
            startDate = resultSet.getString("start_date"),
            endDate = resultSet.getString("end_date")
        )
        contracts.add(contract)
    }

    println("Retrieved Contracts: $contracts")  // Debug statement
    return contracts
}

class ContractToString : SimpleFunction<Contract, String>() {
    override fun apply(contract: Contract): String {
        return "Contract ID: ${contract.id}, Parent ID: ${contract.parentId}, Name: ${contract.contractName}, Value: ${contract.contractValue}, Start Date: ${contract.startDate}, End Date: ${contract.endDate}"
    }
}

fun main() {
    val dbUrl = "jdbc:sqlite:contracts.db"
    val tempFilePath = "contracts"
    val options = PipelineOptionsFactory.create()
    val pipeline = Pipeline.create(options)

    // Register the coders
    val coderRegistry: CoderRegistry = pipeline.coderRegistry
    coderRegistry.registerCoderForClass(Contract::class.java, ContractCoder.of())
    coderRegistry.registerCoderForClass(ParentContract::class.java, ParentContractCoder.of())

    // Step 1: Read Contracts from the Database
    val contracts: List<Contract> = getAllContracts(dbUrl)
    val contractsPCollection: PCollection<Contract> = pipeline.apply("GetContracts", Create.of(contracts)).setCoder(ContractCoder.of())

    // Step 2: Convert Contract objects to strings
    val contractStrings: PCollection<String> = contractsPCollection.apply(
        "ConvertContractsToString",
        MapElements.via(ContractToString())
    )

    // Step 3: Write the contracts to an output file
    contractStrings.apply(
        "WriteContracts",
        TextIO.write().to(tempFilePath).withoutSharding().withSuffix(".log")
    )

    // Run the pipeline
    val result = pipeline.run()
    result.waitUntilFinish()
}