package xuwei.tech.streaming.streamApi

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import xuwei.tech.streaming.customSource.MyNoParallelSourceScala

object StreamingDemoSplitScala {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyNoParallelSourceScala)
    //把流进行分类
    val splitStream = text.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if(value %2 ==0){
          list.add("even")
        }else{
          list.add("odd")
        }
        list
      }
    })
    //选择偶数流
    val evenStream = splitStream.select("even")
    evenStream.print().setParallelism(1)

    env.execute("split")
  }

}
