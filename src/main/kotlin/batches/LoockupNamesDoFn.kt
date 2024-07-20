package batches

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import utils.DatabaseUtils
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.metrics.Metrics

// DoFn to lookup names based on IDs
class LookupNamesDoFn(private val dbUrl: String) : DoFn<KV<String, Iterable<@JvmWildcard Int>>, President>() {
    // Counter to keep track of the number of presidents processed
    private val counter: Counter = Metrics.counter(LookupNamesDoFn::class.java, "president-counter")

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val input = context.element()
        val ids = input.value.joinToString(", ") { it.toString() }
        println("Processing IDs: $ids")  // Debug statement

        // SQL query to get names of presidents
        val query = "SELECT first_name, second_name, start_year, end_year FROM presidents WHERE id IN ($ids)"
        val names = mutableListOf<President>()

        // Execute the query and collect names
        DatabaseUtils.executeSqlQuery(dbUrl, query) { resultSet ->
            val firstName = resultSet.getString("first_name")
            val lastName = resultSet.getString("second_name")
            val startYear = resultSet.getInt("start_year")
            val endYear = resultSet.getInt("end_year")
            names.add(President("$firstName $lastName", startYear, endYear, 0))
        }

        // Output each name and increment the counter
        for (name in names) {
            context.output(name)
            counter.inc()
        }
    }
}