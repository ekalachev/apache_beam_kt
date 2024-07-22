package batches.contracts

import batches.contracts.coders.ContractCoder
import batches.contracts.coders.FullContractCoder
import batches.contracts.coders.ParentContractCoder
import batches.contracts.model.Contract
import batches.contracts.model.FullContract
import batches.contracts.model.ParentContract
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import utils.DatabaseUtils

/**
 * Retrieves all contracts from the database.
 *
 * @param dbUrl The database URL.
 * @return A list of all contracts.
 */
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

/**
 * Retrieves a parent contract by its ID.
 *
 * @param dbUrl The database URL.
 * @param parentId The ID of the parent contract.
 * @return The parent contract, or null if not found.
 */
fun getParentContractById(dbUrl: String, parentId: Int): ParentContract? {
    var parentContract: ParentContract? = null
    val query = "SELECT id, name, description FROM ParentContracts WHERE id = ?"

    DatabaseUtils.executeSqlQueryWithParams(dbUrl, query, listOf(parentId)) { resultSet ->
        parentContract = ParentContract(
            id = resultSet.getInt("id"),
            name = resultSet.getString("name"),
            description = resultSet.getString("description")
        )
    }

    return parentContract
}

/**
 * Converts a FullContract object to a string representation.
 */
class FullContractToString : SimpleFunction<FullContract, String>() {
    override fun apply(contract: FullContract): String {
        return "Name: ${contract.contractName}, Value: ${contract.contractValue}, Start Date: ${contract.startDate}, End Date: ${contract.endDate}, Parent Name: ${contract.parentName}, Parent Description: ${contract.parentDescription}"
    }
}

/**
 * Groups contracts by their parent IDs.
 */
class GroupByParentId : DoFn<Contract, KV<Int, Contract>>() {
    @ProcessElement
    fun processElement(context: ProcessContext) {
        val contract = context.element()
        context.output(KV.of(contract.parentId, contract))
    }
}

/**
 * Batches contracts by parent IDs.
 */
class BatchGroupByParentId :
    PTransform<PCollection<KV<Int, Contract>>, PCollection<KV<Int, Iterable<Contract>>>>() {
    override fun expand(input: PCollection<KV<Int, Contract>>): PCollection<KV<Int, Iterable<Contract>>> {
        return input.apply(GroupByKey.create())
    }
}

/**
 * Fetches parent contract details and combines them with their corresponding contracts.
 */
class FetchParentContractDetails(private val dbUrl: String) :
    DoFn<KV<Int, Iterable<Contract>>, FullContract>() {
    @ProcessElement
    fun processElement(context: ProcessContext) {
        val parentId = context.element().key
        val contracts = context.element().value

        val parentContract = getParentContractById(dbUrl, parentId)
        if (parentContract != null) {
            for (contract in contracts) {
                context.output(
                    FullContract(
                        contractName = contract.contractName,
                        contractValue = contract.contractValue,
                        startDate = contract.startDate,
                        endDate = contract.endDate,
                        parentName = parentContract.name,
                        parentDescription = parentContract.description
                    )
                )
            }
        }
    }
}

fun main() {
    val dbUrl = "jdbc:sqlite:contracts.db"
    val outputFilePath = "contracts"
    val options = PipelineOptionsFactory.create()
    val pipeline = Pipeline.create(options)

    // Register the coders
    val coderRegistry: CoderRegistry = pipeline.coderRegistry
    coderRegistry.registerCoderForClass(Contract::class.java, ContractCoder.of())
    coderRegistry.registerCoderForClass(ParentContract::class.java, ParentContractCoder.of())
    coderRegistry.registerCoderForClass(FullContract::class.java, FullContractCoder.of())

    // Step 1: Read Contracts from the Database
    val contracts: List<Contract> = getAllContracts(dbUrl)

    val contractsPCollection: PCollection<Contract> = pipeline
        .apply("GetContracts", Create.of(contracts))

    // Step 2: Group contracts by parent IDs
    val groupedByParentId: PCollection<KV<Int, Contract>> = contractsPCollection
        .apply("GroupByParentId", ParDo.of(GroupByParentId()))

    // Step 3: Batch groups of contracts by parent IDs
    val groupedBatches: PCollection<KV<Int, Iterable<Contract>>> = groupedByParentId
        .apply("BatchGroupByParentId", BatchGroupByParentId())

    // Step 4: Fetch parent contract details and combine with contracts
    val parentDetailsWithContracts: PCollection<FullContract> = groupedBatches
        .apply("FetchParentContractDetails", ParDo.of(FetchParentContractDetails(dbUrl)))

    // Step 5: Convert FullContract to string
    val fullContractStrings: PCollection<String> = parentDetailsWithContracts
        .apply("ConvertFullContractToString", MapElements.via(FullContractToString()))

    // Step 6: Save the result to contracts.log
    fullContractStrings.apply(
        "WriteContracts",
        TextIO.write().to(outputFilePath).withoutSharding().withSuffix(".log")
    )

    // Run the pipeline
    val result = pipeline.run()
    result.waitUntilFinish()
}