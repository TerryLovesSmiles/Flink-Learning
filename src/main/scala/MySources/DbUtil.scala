package MySources

import scala.util.Random

object DbUtil {
//模拟一个数据库连接的工具类

  //获取连接
  def getConnection()={
    "得到一个连接"+(new  Random().nextInt(10))
  }

  //模拟一个返回连接
  def returnConnection(connection:String)={
    "关闭连接"
  }
}
