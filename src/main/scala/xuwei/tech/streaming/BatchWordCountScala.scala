package xuwei.tech.streaming

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\data\\inputdir"
    val outputFile = "D:\\data\\outputfile"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    //引入隐式转换
    import org.apache.flink.api.scala._
    val count = text.flatMap(_.toLowerCase.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sortGroup(1, Order.ASCENDING)
      .sum(1)

    count.writeAsCsv(outputFile,"\n"," ").setParallelism(1)
    env.execute("scala batch word count")
  }

}
