package batches.presidents

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

// Helper function to get all IDs from the database
fun getAllIds(dbUrl: String): List<Int> {
    val ids = mutableListOf<Int>()
    val query = "SELECT id FROM presidents"
    DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
        ids.add(resultSet.getInt("id"))
    }
    println("Retrieved IDs: $ids")  // Debug statement
    return ids
}

fun main() {
    val dbUrl = "jdbc:sqlite:presidents.db"
    val tempFilePath = "output"
    val options = PipelineOptionsFactory.create()
    val pipeline = Pipeline.create(options)

    // Step 1: Read IDs from the Database
    val idsPCollection = pipeline.apply("GetIds", Create.of(getAllIds(dbUrl)))

    // Step 2: Assign Batch Key
    val keyedIds = idsPCollection.apply("KeyByConstant", WithKeys.of { _: Int -> "batch" })
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))

    // Step 3: Group IDs into Batches
        val batchedIds = keyedIds.apply("BatchIds", GroupIntoBatches.ofSize(10))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(VarIntCoder.of())))

    // Step 4: Lookup President Names
    val presidentNames = batchedIds.apply("LookupNames", ParDo.of(LookupNamesDoFn(dbUrl)))
        .setCoder(PresidentCoder.of())

    // Step 5: Map President to KV<Name, YearsInOffice>
    val presidentKV: PCollection<KV<String, Long>> = presidentNames.apply(
        "MapToKV",
        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
            .via(SerializableFunction { president: President ->
                KV.of(
                    president.name,
                    (president.endYear - president.startYear).toLong()
                )
            })
    )
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

    // Step 6: Sum Years in Office by Name
    val presidentYearsSum: PCollection<KV<String, Long>> =
        presidentKV.apply("SumYearsInOffice", Combine.perKey(Sum.ofLongs()))

    // Step 7: Format the Counts
    val formattedPresidentYearsSum: PCollection<String> =
        presidentYearsSum.apply(
            "FormatCounts", MapElements.into(TypeDescriptors.strings())
                .via(SerializableFunction { kv: KV<String, Long> -> "${kv.key}: ${kv.value} years in office." })
        )

    // Step 8: Write the Counts to an Output File
    formattedPresidentYearsSum.apply(
        "WritePresidentCounts",
        TextIO.write().to(tempFilePath).withoutSharding().withSuffix(".log")
    )
    // Run the pipeline
    val result = pipeline.run()
    result.waitUntilFinish()

    // Retrieve and print the counter value
    val metricResults = result.metrics()
    val filter = MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.named(LookupNamesDoFn::class.java, "president-counter"))
        .build()
    val counterResults = metricResults.queryMetrics(filter).counters
    val counterValue = counterResults.find { it.name.name == "president-counter" }
    println("Number of presidents processed: ${counterValue?.committed?.toLong() ?: 0}")
}