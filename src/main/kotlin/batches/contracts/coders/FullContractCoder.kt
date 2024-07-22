package batches.contracts.coders

import batches.contracts.model.FullContract
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.DoubleCoder
import java.io.InputStream
import java.io.OutputStream

class FullContractCoder private constructor() : CustomCoder<FullContract>() {

    override fun encode(value: FullContract, outStream: OutputStream) {
        StringUtf8Coder.of().encode(value.contractName, outStream)
        DoubleCoder.of().encode(value.contractValue, outStream)
        StringUtf8Coder.of().encode(value.startDate, outStream)
        StringUtf8Coder.of().encode(value.endDate, outStream)
        StringUtf8Coder.of().encode(value.parentName, outStream)
        StringUtf8Coder.of().encode(value.parentDescription, outStream)
    }

    override fun decode(inStream: InputStream): FullContract {
        val contractName = StringUtf8Coder.of().decode(inStream)
        val contractValue = DoubleCoder.of().decode(inStream)
        val startDate = StringUtf8Coder.of().decode(inStream)
        val endDate = StringUtf8Coder.of().decode(inStream)
        val parentName = StringUtf8Coder.of().decode(inStream)
        val parentDescription = StringUtf8Coder.of().decode(inStream)
        return FullContract(contractName, contractValue, startDate, endDate, parentName, parentDescription)
    }

    override fun verifyDeterministic() {}

    companion object {
        fun of(): FullContractCoder = FullContractCoder()
    }
}