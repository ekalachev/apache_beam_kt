package batches

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import utils.DatabaseUtils
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.metrics.Metrics

// DoFn to lookup names based on IDs
class LookupNamesDoFn(private val dbUrl: String) : DoFn<KV<String, Iterable<@JvmWildcard Int>>, String>() {
    // Counter to keep track of the number of presidents processed
    private val counter: Counter = Metrics.counter(LookupNamesDoFn::class.java, "president-counter")

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val input = context.element()
        val ids = input.value.joinToString(", ") { it.toString() }
        println("Processing IDs: $ids")  // Debug statement

        // SQL query to get names of presidents
        val query = "SELECT first_name, second_name FROM presidents WHERE id IN ($ids)"
        val names = mutableListOf<String>()

        // Execute the query and collect names
        DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
            val firstName = resultSet.getString("first_name")
            val lastName = resultSet.getString("second_name")
            names.add("$firstName $lastName")
        }

        // Output each name and increment the counter
        for (name in names) {
            context.output(name)
            counter.inc()
        }
    }
}