package pingrunhuang.github.io.common

import org.apache.spark.Partitioner
import pingrunhuang.github.io.geometry.IGeo

/**
  * @ author Frank Huang
  * @ date 2018/12/16 
  */
class GeoPartitioner(nums:Int) extends Partitioner{
  override def numPartitions: Int = nums

  override def getPartition(key: Any): Int = {
    key match {
      case geo: IGeo => geo.district_code % nums
      case _ => throw new Exception("Type miss match")
    }
  }
}
