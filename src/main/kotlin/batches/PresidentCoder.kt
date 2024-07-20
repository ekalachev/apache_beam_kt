package batches

import org.apache.beam.sdk.coders.AtomicCoder
import java.io.*

class PresidentCoder : AtomicCoder<President>() {

    companion object {
        private val instance: PresidentCoder = PresidentCoder()

        fun of(): PresidentCoder {
            return instance
        }
    }

    override fun encode(value: President, outStream: OutputStream) {
        val dataOutStream = DataOutputStream(outStream)
        val nameBytes = value.name.toByteArray(Charsets.UTF_8)
        dataOutStream.writeInt(nameBytes.size)
        dataOutStream.write(nameBytes)
        dataOutStream.writeInt(value.startYear)
        dataOutStream.writeInt(value.endYear)
        dataOutStream.writeInt(value.yearsInOffice)
    }

    override fun decode(inStream: InputStream): President {
        val dataInStream = DataInputStream(inStream)
        val nameLength = dataInStream.readInt()
        val nameBytes = ByteArray(nameLength)
        dataInStream.readFully(nameBytes)
        val name = String(nameBytes, Charsets.UTF_8)
        val startYear = dataInStream.readInt()
        val endYear = dataInStream.readInt()
        val yearsInOffice = dataInStream.readInt()
        return President(name, startYear, endYear, yearsInOffice)
    }
}