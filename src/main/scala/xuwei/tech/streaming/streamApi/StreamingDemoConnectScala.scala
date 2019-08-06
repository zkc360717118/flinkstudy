package xuwei.tech.streaming.streamApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.streaming.customSource.MyNoParallelSourceScala

object StreamingDemoConnectScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)
    val text2_str = text2.map(_+"hah") // 转化成字符串

    val connectStream  = text1.connect(text2_str)
    val result = connectStream.map(line1=>{line1},line2=>{line2})
    result.print().setParallelism(1)

    env.execute("connect 算子的使用")
  }

}
