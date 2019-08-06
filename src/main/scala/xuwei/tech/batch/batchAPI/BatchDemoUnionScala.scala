package xuwei.tech.batch.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoUnionScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))


    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"jack"))
    data2.append((2,"lili"))
    data2.append((3,"jessic"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.union(text2).print()
//    (1,zs)
//    (1,jack)
//    (2,ls)
//    (2,lili)
//    (3,ww)
//    (3,jessic)
  }
}
