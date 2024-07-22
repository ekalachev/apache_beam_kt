package batches.contracts.coders

import batches.contracts.model.Contract
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.coders.DoubleCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.util.VarInt
import java.io.InputStream
import java.io.OutputStream

class ContractCoder private constructor() : CustomCoder<Contract>() {

    override fun encode(value: Contract, outStream: OutputStream) {
        VarInt.encode(value.id, outStream)
        VarInt.encode(value.parentId, outStream)
        StringUtf8Coder.of().encode(value.contractName, outStream)
        DoubleCoder.of().encode(value.contractValue, outStream)
        StringUtf8Coder.of().encode(value.startDate, outStream)
        StringUtf8Coder.of().encode(value.endDate, outStream)
    }

    override fun decode(inStream: InputStream): Contract {
        val id = VarInt.decodeInt(inStream)
        val parentId = VarInt.decodeInt(inStream)
        val contractName = StringUtf8Coder.of().decode(inStream)
        val contractValue = DoubleCoder.of().decode(inStream)
        val startDate = StringUtf8Coder.of().decode(inStream)
        val endDate = StringUtf8Coder.of().decode(inStream)
        return Contract(id, parentId, contractName, contractValue, startDate, endDate)
    }

    override fun verifyDeterministic() {}

    companion object {
        fun of(): ContractCoder = ContractCoder()
    }
}