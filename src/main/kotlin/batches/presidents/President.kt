package batches.presidents

data class President(
    val name: String,
    val startYear: Int,
    val endYear: Int,
    val yearsInOffice: Int
) {
    override fun hashCode(): Int {
        return name.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is President) return false
        return name == other.name
    }
}