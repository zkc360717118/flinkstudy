package xuwei.tech.batch.batchAPI

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoMapPartitionScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = ListBuffer[String]()
    data.append("hello you")
    data.append("hello me")

//    val text = env.fromCollection(data).setParallelism(2)
//    text.map(x=>{
//      println("线程："+Thread.currentThread().getId+"  value:"+x)
//    }).print()
//    text.mapPartition(it => {
//      //创建数据库连接，建议吧这块代码放到try-catch代码块中
//      val res = ListBuffer[String]()
//      while(it.hasNext){
//        System.out.println("线程："+Thread.currentThread().getId)
//        val line = it.next()
//        System.out.println("数据："+line)
//        val words = line.split("\\W+")
//        for(word <- words){
//          res.append(word)
//        }
//      }
//      res
//    }).print()
  }
}
