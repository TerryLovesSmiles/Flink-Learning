package MySources

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
//引入隐式转换,否则会报错
import  org.apache.flink.api.scala._
object SourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //调用
    //sourceCollection(env)

   // sourceFile(env)
    //sourceFile2(env)
   // sourceCSV(env)
    sourceCompressed(env)
  }

  //数据源的获取
  //1 从集合中获取      fromCollection
  def sourceCollection(env:ExecutionEnvironment)={
    //val data=1 to 10
    val data=List(1,2,3)

    env.fromCollection(data).print()
  }

  //2.1 从文件获取
  def sourceFile(env:ExecutionEnvironment)={
   val data= env.readTextFile("D:\\wc.txt")
    data.print()
  }

  //2.2遍历文件夹获取
  def sourceFile2(env:ExecutionEnvironment) ={
    val configuration = new Configuration()
    configuration.setBoolean("recursive.file.enumeration",true) //固定写法,开启递归
    env.readTextFile("D:\\test").withParameters(configuration).print()
  }

  //3 从csv(excell文件中)读
  def sourceCSV(env:ExecutionEnvironment)={
   val data= env.readCsvFile[MyPeople]("D:\\Terry内网通\\flink文档\\9.24\\flink-b\\people.csv",ignoreFirstLine = true)
    data.print()
  }

  //样例类
  case class MyPeople(name:String,age:Int,job:String)

  //4 读取压缩文件
  def sourceCompressed(env:ExecutionEnvironment)={
    env.readTextFile("D:\\test\\wc.rar").print()
  }

}
