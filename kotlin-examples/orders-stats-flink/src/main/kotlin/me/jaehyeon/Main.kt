package me.jaehyeon

fun main(args: Array<String>) {
    when (args.getOrNull(0)?.lowercase()) {
        "datastream" -> DataStreamApp.run()
        "table" -> TableApp.run()
        else -> println("Usage: <datastream | table>")
    }
}
