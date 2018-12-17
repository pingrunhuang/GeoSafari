package pingrunhuang.github.io

import java.sql.Timestamp

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils
import org.locationtech.spatial4j.io.GeohashUtils

/**
  * @ author Frank Huang
  * @ date 2018/12/13 
  */
class PoiFacility(spark:SparkSession) {
  val PRECISION = 6
  val geoFactory = new GeometryFactory()

  case class Grid(grid_code:String, polygon:Polygon) extends Serializable

  /**
    * Given a poi table that has a column called location, generate broadcast variable with key is geohash
    * */
  def store2broadcast(poi_table:DataFrame, numPartitions:Int=3):Broadcast[Map[String, Row]]={
    val store_geohash = poi_table.rdd.map(row=>{
      val location = row.getAs[String]("location")
      val geohash = GeohashUtils.encodeLatLon(
        location.split(",").head.toDouble, location.split(",").last.toDouble,
        PRECISION)

      (geohash, row)
    }).collect().toMap
    spark.sparkContext.broadcast(store_geohash)
  }

  /**
    * Given a grid table that contains columns grid_code, left_down, right_down, right_up and left_up
    * generate broadcast variable (geohash, grid_code)
    * */
  def grid2Broadcast(grids:DataFrame, numPartitions:Int=3):Broadcast[Map[String, String]]={
    val store_geohash = grids.rdd.map(row=>{
      val grid_code = row.getAs[String]("grid_code")
      val left_down = row.getAs[String]("left_down")
      val right_down = row.getAs[String]("right_down")
      val right_up  = row.getAs[String]("right_up")
      val left_up   = row.getAs[String]("left_up")

      val polygon = geoFactory.createPolygon(
        Array(
          new Coordinate(left_down.split(",").head.toDouble, left_down.split(",").last.toDouble),
          new Coordinate(right_down.split(",").head.toDouble, right_down.split(",").last.toDouble),
          new Coordinate(right_up.split(",").head.toDouble, right_up.split(",").last.toDouble),
          new Coordinate(left_up.split(",").head.toDouble, left_up.split(",").last.toDouble),
          new Coordinate(left_down.split(",").head.toDouble, left_down.split(",").last.toDouble)
        )
      )
      val geohash = GeoHashUtils.encode(polygon, PRECISION)
      (geohash, grid_code)
    }).collect().toMap

    spark.sparkContext.broadcast(store_geohash)
  }

  /**
    * Depends on your precision, each geohash will correspond to several grids
    * */
  def gridCenter2Broadcast(grids:DataFrame, numPartitions:Int=3):Broadcast[Map[String, Seq[Grid]]]={
    val store_geohash   = grids.rdd.map(row=>{
      val grid_code     = row.getAs[String]("grid_code")
      val grid_center   = row.getAs[String]("center")
      val left_down     = row.getAs[String]("left_down")
      val right_down    = row.getAs[String]("right_down")
      val right_up      = row.getAs[String]("right_up")
      val left_up       = row.getAs[String]("left_up")

      val polygon = geoFactory.createPolygon(
        Array(
          new Coordinate(left_down.split(",").head.toDouble, left_down.split(",").last.toDouble),
          new Coordinate(right_down.split(",").head.toDouble, right_down.split(",").last.toDouble),
          new Coordinate(right_up.split(",").head.toDouble, right_up.split(",").last.toDouble),
          new Coordinate(left_up.split(",").head.toDouble, left_up.split(",").last.toDouble),
          new Coordinate(left_down.split(",").head.toDouble, left_down.split(",").last.toDouble)
        )
      )

      val geohash = GeohashUtils.encodeLatLon(grid_center.split(",").head.toDouble, grid_center.split(",").last.toDouble, PRECISION)

      (geohash, Grid(grid_code, polygon))

    }).groupByKey(numPartitions).map(row=>(row._1, row._2.toSeq)).collect().toMap

    spark.sparkContext.broadcast(store_geohash)
  }



  def poi_match_grid(poi:DataFrame, grids:Broadcast[Map[String, Seq[Grid]]]):RDD[Row]= {
    val rdd = poi.rdd.map(row => {
      val name = row.getAs[String]("name")
      val city_code = row.getAs[String]("city_code")
      val city_name = row.getAs[String]("city_name")
      val address = row.getAs[String]("address")
      val brand_id = row.getAs[Int]("brand_id")
      val store_type_id = row.getAs[Int]("store_type_id")
      val location = row.getAs[String]("location")
      val status = row.getAs[Int]("status")
      val create_time = row.getAs[Timestamp]("create_time")
      val update_time = row.getAs[Timestamp]("update_time")
      val state = row.getAs[Int]("state")
      val on_date = row.getAs[Timestamp]("on_date")
      val off_date = row.getAs[Timestamp]("off_date")

      val store_point = geoFactory.createPoint(new Coordinate(location.split(",").head.toDouble, location.split(",").last.toDouble))
      val geohash = GeohashUtils.encodeLatLon(location.split(",").head.toDouble, location.split(",").last.toDouble, PRECISION)

      val same_geohash_grids = grids.value.getOrElse[Seq[Grid]](geohash, Seq())

      val matched_grids = same_geohash_grids.map(grid => {
        if (grid.polygon.contains(store_point)) {
          (true, grid)
        } else {
          (false, grid)
        }
      }).filter(_._1).map(_._2)

      /*store did not match any grids*/
      if (matched_grids.isEmpty) {
        Row(name, city_code, city_name, address, brand_id, store_type_id, location, status, create_time, update_time, state, on_date, off_date, "")
      } else {
        Row(name, city_code, city_name, address, brand_id, store_type_id, location, status, create_time, update_time, state, on_date, off_date, matched_grids.head.grid_code)
      }
    })
    rdd
  }
}
