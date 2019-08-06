package xuwei.tech.streaming.streamApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.streaming.customSource.MyNoParallelSourceScala

object StreamingDemoMyPartitionerScala {
  def main(args: Array[String]): Unit = {
    //隐式转换
    import  org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.addSource(new MyNoParallelSourceScala)

    //把long类型的数据转成tuple类型
    val tupleData = text.map(line=>{
      Tuple1(line)
    })

    //虽然默认我笔记本有4核 所以有4个分区，但是由于下面的自定分区只会产生2个，所以，结果只会有2个不一样的线程id，交替执行。
    val partitionData = tupleData.partitionCustom(new MyPartitionerScala,0)
    val result = partitionData.map(line=>{
      println("当前线程id:" + Thread.currentThread().getId+"  value:"+line)
      line._1
    })

    result.print().setParallelism(1)
    env.execute("自定义分区")
  }

}
