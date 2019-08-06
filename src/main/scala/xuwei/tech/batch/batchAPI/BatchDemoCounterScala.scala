package xuwei.tech.batch.batchAPI

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoCounterScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements("a","b","c","d")
    val res = data.map(new RichMapFunction[String,String] {
      //1.定义累加器
      val numLines = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        // 2 注册累加器
        getRuntimeContext.addAccumulator("num-lines",numLines)
      }

      override def map(value: String): String = {
        numLines.add(1)
        value
      }
    }).setParallelism(4)

    //写出结果
    res.writeAsText("d:\\data\\count21")
    val jobResult = env.execute("测试累加器")
    // 获取累加器
    val num = jobResult.getAccumulatorResult[Int]("num-lines")
    println("num:"+num)

  }
}
