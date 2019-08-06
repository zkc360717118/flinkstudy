package xuwei.tech.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo.ExecutionExceptionInfo

/**
  * 笛卡尔积
  */
object BatchDemoCrossScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = List("zs","ww")
    val data2 = List(1,2)

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.cross(text2).print()
  }
}
