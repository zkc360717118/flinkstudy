package xuwei.tech.streaming.streamApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import xuwei.tech.streaming.customSource.MyNoParallelSourceScala

object StreamingDemoUnionScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)
    val unionData = text1.union(text2)

    val sum = unionData.map(line=>{
      println("接收到的数据："+line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sum .print().setParallelism(1)
    env.execute("union 测试")
  }

}
