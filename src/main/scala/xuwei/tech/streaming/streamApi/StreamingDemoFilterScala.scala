package xuwei.tech.streaming.streamApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import xuwei.tech.streaming.customSource.MyNoParallelSourceScala

object StreamingDemoFilterScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyNoParallelSourceScala)
    val mapData = text.map(line =>{
      println("原始接收到的数据："+line)
      line
    }).filter(_ % 2 ==0)

    val result =  mapData.map(line=>{
      println("过滤之后的数据："+line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    result.print().setParallelism(1)

    env.execute("connect 算子的使用")
  }

}
