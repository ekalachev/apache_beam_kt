package batches.contracts

import batches.contracts.coders.ContractCoder
import batches.contracts.coders.FullContractCoder
import batches.contracts.coders.ParentContractCoder
import batches.contracts.model.Contract
import batches.contracts.model.FullContract
import batches.contracts.model.ParentContract
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.*
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
 * Retrieves a list of parent contracts by their IDs.
 *
 * @param dbUrl The database URL.
 * @param parentIds The list of parent IDs.
 * @return A map of parent IDs to their corresponding parent contracts.
 */
fun getParentContractsByIds(dbUrl: String, parentIds: List<Int>): Map<Int, ParentContract> {
    val parentContracts = mutableMapOf<Int, ParentContract>()
    val query = "SELECT id, name, description FROM ParentContracts WHERE id IN (${parentIds.joinToString(",")})"

    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        val parentContract = ParentContract(
            id = resultSet.getInt("id"),
            name = resultSet.getString("name"),
            description = resultSet.getString("description")
        )
        parentContracts[parentContract.id] = parentContract
    }

    return parentContracts
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
 * Fetches parent contract details and combines them with their corresponding contracts.
 */
class FetchParentContractDetails(private val dbUrl: String) :
    DoFn<KV<String, Iterable<@JvmWildcard KV<Int, Iterable<@JvmWildcard Contract>>>>, FullContract>() {
    @ProcessElement
    fun processElement(context: ProcessContext) {
        val input = context.element()

        // Collect parent IDs
        val parentIds = input.value.map { it.key }

        // Fetch parent contract details in a single query
        val parentContracts = getParentContractsByIds(dbUrl, parentIds)

        val contracts = input.value.flatMap { it.value }

        // Combine parent contract details with contracts
        for (contract in contracts) {
            val parentContract = parentContracts[contract.parentId]

            if (parentContract != null) {
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

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        // Optional: Implement logic to handle actions at the end of processing a bundle, if needed.
    }
}

fun main() {
    val dbUrl = "jdbc:sqlite:contracts.db"
    val outputFilePath = "contracts"
    val batchSize: Long = 10
    val options = PipelineOptionsFactory.create()
    val pipeline = Pipeline.create(options)

    // Register the coders
    val coderRegistry: CoderRegistry = pipeline.coderRegistry
    coderRegistry.registerCoderForClass(Contract::class.java, ContractCoder.of())
    coderRegistry.registerCoderForClass(ParentContract::class.java, ParentContractCoder.of())
    coderRegistry.registerCoderForClass(FullContract::class.java, FullContractCoder.of())

    // Step 1: Read contracts from the database
    val contracts: List<Contract> = getAllContracts(dbUrl)

    val contractsPCollection: PCollection<Contract> = pipeline
        .apply("GetContracts", Create.of(contracts))

    // Step 2: Group contracts by parent IDs
    val groupedByParentId: PCollection<KV<Int, Contract>> = contractsPCollection
        .apply("GroupByParentId", ParDo.of(GroupByParentId()))

    // Step 3: Batch parent IDs into fixed-size batches
    val batchedByParentId: PCollection<KV<String, Iterable<KV<Int, Iterable<Contract>>>>> = groupedByParentId
        .apply("GroupByKey", GroupByKey.create())
        .apply("SetBatchKey", WithKeys.of { _: KV<Int, Iterable<Contract>> -> "batch" })
        .setCoder(KvCoder.of(StringUtf8Coder.of(), KvCoder.of(VarIntCoder.of(), IterableCoder.of(ContractCoder.of()))))
        .apply("GroupIntoBatches", GroupIntoBatches.ofSize(batchSize))

    // Step 4: Fetch parent contract details and combine with contracts
    val parentDetailsWithContracts: PCollection<FullContract> = batchedByParentId
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