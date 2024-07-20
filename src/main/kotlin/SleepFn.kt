import org.apache.beam.sdk.transforms.DoFn

class SleepFn(private val sleepDurationMs: Long) : DoFn<Void, Void>() {
    @ProcessElement
    fun processElement() {
        Thread.sleep(sleepDurationMs)
    }
}