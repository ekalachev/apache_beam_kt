package batches.contracts.coders

import batches.contracts.model.ParentContract
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.util.VarInt
import java.io.InputStream
import java.io.OutputStream

class ParentContractCoder private constructor() : CustomCoder<ParentContract>() {

    override fun encode(value: ParentContract, outStream: OutputStream) {
        VarInt.encode(value.id, outStream)
        StringUtf8Coder.of().encode(value.name, outStream)
        StringUtf8Coder.of().encode(value.description, outStream)
    }

    override fun decode(inStream: InputStream): ParentContract {
        val id = VarInt.decodeInt(inStream)
        val name = StringUtf8Coder.of().decode(inStream)
        val description = StringUtf8Coder.of().decode(inStream)
        return ParentContract(id, name, description)
    }

    override fun verifyDeterministic() {}

    companion object {
        fun of(): ParentContractCoder = ParentContractCoder()
    }
}