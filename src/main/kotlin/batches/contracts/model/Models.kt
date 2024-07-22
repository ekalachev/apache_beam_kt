package batches.contracts.model

data class ParentContract(
    val id: Int,
    val name: String,
    val description: String
)

data class Contract(
    val id: Int,
    val parentId: Int,
    val contractName: String,
    val contractValue: Double,
    val startDate: String,
    val endDate: String
)