package xuwei.tech.streaming.customSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithMyRichParallelSourceScala {
  def main(args: Array[String]): Unit = {
    //隐式转换
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.addSource(new MyRichParallelSourceScala)
    val mapData = text.map(line => {
      println("get data:"+ line)
      line
    })

    mapData.timeWindowAll(Time.seconds(2)).sum(0) //每2秒查询一次总数
    mapData.print().setParallelism(1)
    env.execute("自定义单行source")
  }

}
