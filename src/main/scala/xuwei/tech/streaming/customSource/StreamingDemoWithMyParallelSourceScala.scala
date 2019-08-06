package xuwei.tech.streaming.customSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithMyParallelSourceScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyParallelSourceScala).setParallelism(2)
    val mapData = text.map(line=>{
      println("get data:"+ line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("并行自定义source")
  }
}
