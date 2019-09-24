package MySources

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer
//引入隐式转换,否则会报错
import  org.apache.flink.api.scala._
object Transformations {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //tmap(env)
    //tflatMap(env)
    //tFilter(env)
    //tmapPartition(env)
    tFirst(env)
   // tWc(env)
    //tDisTinct(env)
    //tJoin(env)
    //tOutJoin(env)
  }

  //算子
  //1  map   取一个元素产生一个元素
  def tmap(env:ExecutionEnvironment)={
    val data1=1 to 10
    val result = env.fromCollection(data1)
    //作用上算子
    //result.map(x=>x+1).print()
    result.map(_+1).print()
  }

  //2 flatMap算子    取一个元素并产生零个，一个或多个元素。
  def tflatMap(env:ExecutionEnvironment)={
    env.setParallelism(1)
    val result = env.readTextFile("D:\\test\\wc.txt")
    result.flatMap(x=>x.split("\t")).print()
  }

  //3 Filter算子  过滤
    def tFilter(env:ExecutionEnvironment)={
    //val data=1 to 10
    val value = env.fromCollection(List(1,3,4,6,7,1,8,9))
    value.filter(x=>x>4).print()
  }

  //4 mapPartition
  def tmapPartition(env:ExecutionEnvironment)={
    //先用map
    //模拟将100个数据存入数据库
    val data=1 to 100
    val rel=env.fromCollection(data)
    //map是一个数据产生一个新数据
   /* rel.map(x=>{
      //
     val str= DbUtil.getConnection()
      println(str+"开始存入数据库")
      DbUtil.returnConnection("1")

    }).print()

    //上述代码在存入数据库100个数据的操作中,获取并关闭了100次连接,工作是完成了,但不合理
    */

     rel.mapPartition(x=>{
      val str= DbUtil.getConnection()
      println(str+"开始存入数据库")
      DbUtil.returnConnection("1")

      x
    }).setParallelism(1).print()
    //并行度是1,所有操作使用了获取1次连接,关了1次,x执行了100次
    //并行度是5,所有操作使用了获取5次连接,关了5次,x执行了100次
    //并行度是5类似于开了5个线程,5个同步操作


    //map和mapPartition 区别: 后者多了分区操作
    //map每个元素用一个连接, mapPartition每一个区用一个连接
  }

  //5 tFirst
  def tFirst(env:ExecutionEnvironment)={
    val list=new ListBuffer[(String,Int)]
    list.append(("a",100))
    list.append(("a",200))
    list.append(("b",-30))
    list.append(("b",-20))
    list.append(("c",2))
    list.append(("c",4))
    list.append(("c",3))

    val value = env.fromCollection(list)
    //value.first(3).print()
    //value.groupBy(0).first(2).print()
    value.groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()
  }

  //6 wordCount 单词统计
  def tWc(env:ExecutionEnvironment)={
    val data = env.readTextFile("D:\\test\\b.txt")
    //data.print()
    // 第一步   1变n
    //data.flatMap(x=>x.split("\t")).print()

    //第二步   x->(x,1)
   //data.flatMap(x=>x.split("\t")).map(x=>(x,1)).print()
    //第三步  分组,求和
    data.flatMap(x=>x.split("\t")).map(x=>(x,1)).groupBy(0).sum(1).print()
  }

  //7 DisTinct  去重
  def tDisTinct(env:ExecutionEnvironment)={
    val list=List(1,3,2,1,4,4,5,6,1,1)
    val result = env.fromCollection(list)
    result.distinct().print()
  }

  //8 join

  def tJoin(env:ExecutionEnvironment)={

    val data1 = new ListBuffer[(Int,String)]
    data1.append((1,"小白"))
    data1.append((2,"小红"))
    data1.append((3,"小绿"))

    val data2 = new ListBuffer[(Int,String)]
    data2.append((1,"北京"))
    data2.append((2,"上海"))
    data2.append((3,"苏州"))
    data2.append((4,"南京"))

    env.setParallelism(1)
    //join
    val result1 = env.fromCollection(data1)
    val result2 = env.fromCollection(data2)
    //join 把两个整成1个,但是要加条件
    result1.join(result2).where(0).equalTo(0).map(x=>(x._1._1,x._1._2,x._2._2
    )).print()






  }

  //9  leftOuterJoin 左连接
  def tOutJoin(env:ExecutionEnvironment)={
    val data1 = new ListBuffer[(Int,String)]
    data1.append((1,"小白"))
    data1.append((2,"小红"))
    data1.append((3,"小绿"))
    data1.append((5,"小康"))

    val data2 = new ListBuffer[(Int,String)]
    data2.append((1,"北京"))
    data2.append((2,"上海"))
    data2.append((3,"苏州"))
    data2.append((4,"南京"))

    env.setParallelism(1)
    //join
    val result1 = env.fromCollection(data1)
    val result2 = env.fromCollection(data2)
    //左连接
    result1.leftOuterJoin(result2).where(0).equalTo(0).apply((first,second)=>{
      if (second==null){
        (first._1,first._2,"")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()
  }
}











