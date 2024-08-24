package streaming

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import org.joda.time.Duration
import java.io.Serializable
import kotlin.random.Random

/**
 * Data class representing a user click event.
 *
 * @property seqNum The sequence number associated with the event.
 * @property userId The ID of the user who generated the click event.
 * @property timestamp The timestamp when the click event occurred.
 */
data class UserClick(
    val seqNum: Long,
    val userId: String,
    val timestamp: Long
) : Serializable

/**
 * The main function to run the Apache Beam pipeline.
 *
 * The pipeline simulates a stream of user click events, applies windowing,
 * and writes the processed data to text files.
 */
fun main() {
    // Create a new Pipeline instance.
    val pipeline = Pipeline.create()

    // Generate a sequence of numbers to simulate a continuous stream of data.
    // Each number is generated at a 1-second interval.
    val eventStream = pipeline.apply(
        "GenerateSequence",
        GenerateSequence.from(0)
            .withRate(1, Duration.standardSeconds(1))
    )

    // Map each generated sequence number to a UserClick object.
    // The userId is randomly selected from a pool of 5 users.
    // The timestamp is the current system time when the event is generated.
    val userClicks = eventStream.apply(
        "MapToUserClicks",
        MapElements.into(TypeDescriptor.of(UserClick::class.java))
            .via(object : SimpleFunction<Long, UserClick>() {
                override fun apply(seqNum: Long): UserClick {
                    val userId = "user${Random.nextInt(1, 6)}"
                    val timestamp = System.currentTimeMillis()
                    return UserClick(seqNum, userId, timestamp)
                }
            })
    ).setCoder(SerializableCoder.of(UserClick::class.java))

    // Apply windowing to group UserClick events into fixed windows of 10 seconds.
    // This step is useful for processing and analyzing data in manageable chunks.
    val windowedUserClicks = userClicks.apply(
        "Windowing",
        Window.into(FixedWindows.of(Duration.standardSeconds(10)))
    )

    // Format the UserClick objects as strings for writing to text files.
    // The formatted string includes the sequence number, user ID, and timestamp.
    val formattedUserClicks = windowedUserClicks.apply(
        "FormatUserClicks",
        MapElements.into(TypeDescriptors.strings())
            .via(object : SimpleFunction<UserClick, String>() {
                override fun apply(input: UserClick): String {
                    return "SeqNum: ${input.seqNum}, User: ${input.userId}, Timestamp: ${input.timestamp}"
                }
            })
    )

    // Write the formatted UserClick strings to text files.
    // The output files are written in a windowed manner, with one file per window.
    formattedUserClicks.apply(
        "WriteToText",
        TextIO.write()
            .to("output/user-clicks")
            .withWindowedWrites() // Enable windowed writes to handle streaming data.
            .withNumShards(1) // Specify the number of output files per window.
            .withSuffix(".txt") // Set the file extension for the output files.
    )

    // Execute the pipeline and wait for it to finish.
    pipeline.run().waitUntilFinish()
}