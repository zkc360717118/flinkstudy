package xuwei.tech.streaming.sink


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.ParquetRowInputFormat
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

/**
  * @Author yyb
  * @Description
  * @Date Create in 2019-10-12
  * @Time 15:23
  */
object ReadFromParquetToOracle {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.flink").setLevel(Level.INFO)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hdfs_parquet_file_path_t1 = "hdfs://192.168.0.20:8020/user/hive/warehouse/kevin.db/ph/000000_0"
    val hdfs_parquet_file_path = "hdfs://192.168.0.20:8020/user/hive/warehouse/kevin.db/ph/"


    /**
      * 手动指定 parquet的 schema
      */
    val rule_name = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "rule_name")
    val num = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "num")
    val t1_schema = new MessageType("t1", rule_name, num)
    println(s"t1_schema : ${t1_schema}")
    val t1 = env.readFile(new ParquetRowInputFormat(new Path(hdfs_parquet_file_path), t1_schema), hdfs_parquet_file_path,FileProcessingMode.PROCESS_CONTINUOUSLY,1000)

    //    t1.print().setParallelism(1)
    //    t1.map(_.getField(2)).print().setParallelism(1)
    println("数据类型是："+t1.dataType)
    t1.addSink(new SinkOracle)
    env.execute("ReadFromParquet")

  }
}


class SinkOracle extends RichSinkFunction[Row]
{
  private var connection:Connection = null
  private var statement:PreparedStatement = null
  private val  username = "test"
  private val  password = "test"
  var batchnum = 0  //oracle batch size
var timeDuration:Long = 0 // oracle batch time

  //  1初始化
  override def open(parameters: configuration.Configuration): Unit = {
    //set time duration
    timeDuration = System.currentTimeMillis();

    super.open(parameters)
    Class.forName("oracle.jdbc.driver.OracleDriver")
    connection = DriverManager.getConnection("jdbc:oracle:thin:@47.105.78.73:1521:helowin",username,password)
    connection.setAutoCommit(false)
    //    //这里最好不要使用createStatement,如果原始数据中有单引号 ' ,在sql语句运行的时候会报错
    statement = connection.prepareStatement("insert into TEST.PHPOC values (?,?)")

  }
  // 2 执行
  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
    val name = value.getField(0).asInstanceOf[String]
    val num = value.getField(1).asInstanceOf[String]
    statement.setString(1,name)
    statement.setString(2,num)
    statement.addBatch()
    batchnum = batchnum+1
    statement.executeBatch()
    connection.commit()
    println("来了：")
    print(getRuntimeContext.getAttemptNumber)

    //有问题，如果最后一次没有凑够1000 怎么处理
    //    if (batchnum % 1000 ==0){  //
    //      statement.executeBatch()
    //      connection.commit()
    //      batchnum = 0
    //      timeDuration = System.currentTimeMillis()
    //    }


  }
  //3 关闭
  override def close(): Unit = {
    println("我关闭了")
    super.close()
    if (statement != null) statement.close
    if (connection != null) connection.close
  }
  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext



}
