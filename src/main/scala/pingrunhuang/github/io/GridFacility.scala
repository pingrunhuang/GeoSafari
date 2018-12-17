package pingrunhuang.github.io

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils
import pingrunhuang.github.io.common.GeoPartitioner
import pingrunhuang.github.io.geometry.{Grid, Poi}

/**
  * @ author Frank Huang
  * @ date 2018/12/06 
  */
class GridFacility(spark:SparkSession) extends Serializable {
  private val geoFactory  = new GeometryFactory()
  private val RESULT_PATH = "/home/hadoop/match_result.csv"

  /**
    * This method is useful for when you have more grids then poi to match with partition based on district code
    * */
  def gridBrutalMatchPoints(grid_rdd:RDD[Grid], poi_rdd:RDD[Poi]):RDD[Poi]={

    val district_poi_bd = spark.sparkContext.
      broadcast(poi_rdd.collect().map(poi=>(poi.district_code,poi)).groupBy(e=>e._1))

    val num_districts = grid_rdd.map(grid=>grid.district_code).distinct().count().toInt
    /*partitionBy could could only work on pairRDD
    * ParallelCollectionRDD,CoGroupedRDD,HadoopRDD,MapReduce,MapPartitionsRDD,CoalescedRDD,ShuffledRDD,PipedRDD,PairRDD,DoubleRDD,SequenceFileRDD */
    grid_rdd.map(grid=>(grid.district_code, grid))
      .partitionBy(new GeoPartitioner(num_districts))
      .mapPartitions[Poi](p=>{
        p.map(row => {
          val district_code = row._1
          val left_up = new Coordinate(row._2.up_left.lat, row._2.up_left.lon)
          val left_down = new Coordinate(row._2.down_left.lat, row._2.down_left.lon)
          val right_up = new Coordinate(row._2.up_right.lat, row._2.up_right.lon)
          val right_down = new Coordinate(row._2.down_right.lat, row._2.down_right.lon)
          val polygon = geoFactory.createPolygon(Array(left_up, left_down, right_down, right_up, left_up))
          district_poi_bd.value(district_code).map(poi => {
            val poi_x = poi._2.location.lat
            val poi_y = poi._2.location.lon
            val poi_point = geoFactory.createPoint(new Coordinate(poi_x, poi_y))
            if (polygon.contains(poi_point)) {
              (true, poi, row._2.grid_code)
            } else {
              (false, poi, row._2.grid_code)
            }
          }).filter(_._1)
        }).flatMap(rows => {
          /*address:String, location:Location, district_code:Int, grid_code:String*/
          rows.map(row => Poi(row._2._2.address, row._2._2.location, row._2._1, row._3))
        })
      })
  }


  /**
    * This method is useful for when you have more poi then grids to match with geohash index
    * */
  def gridGeohashMatchPoints(grid_rdd:RDD[Grid], poi_rdd:RDD[Poi], PRECISION:Int):Unit={
    val grids_dict = grid_rdd
      .map(row=>{
        val center = geoFactory.createPoint(new Coordinate(row.center.lat, row.center.lon))
        val geohash = GeoHashUtils.encode(center, PRECISION)
        (geohash, Seq(row))
      })
      .reduceByKey((x,y)=>x ++ y)
      .collectAsMap()

    val grids_dict_bc = spark.sparkContext.broadcast[scala.collection.Map[String, Iterable[Grid]]](grids_dict)

    val result = poi_rdd.map(row=>{
      val poi_x       = row.location.lat
      val poi_y       = row.location.lon
      val poi_point   = geoFactory.createPoint(new Coordinate(poi_x, poi_y))
      val hash_value  = GeoHashUtils.encode(poi_point, PRECISION)

      val match_grids = grids_dict_bc.value.get(hash_value)

      if (match_grids.isDefined){
        val poi_address = row.address
        val matched_grid = match_grids.get.map(g=>{
          val left_up = new Coordinate(g.up_left.lat, g.up_left.lon)
          val left_down = new Coordinate(g.down_left.lat, g.down_left.lon)
          val right_up = new Coordinate(g.up_right.lat, g.up_right.lon)
          val right_down = new Coordinate(g.down_right.lat, g.down_right.lon)
          val polygon = geoFactory.createPolygon(Array(left_up, left_down, right_down, right_up, left_up))
          (polygon, g.center)
        }).filter(bar=>bar._1.contains(poi_point)).head

        (hash_value, poi_address, poi_point, true, matched_grid._1.toString, matched_grid._2.lat, matched_grid._2.lon)
      }else{
        (hash_value, "", poi_point, false, "", "", "")
      }
    }).filter(row=>row._4).map(row=>{
      Row(row._1, row._2, row._3, row._5, row._6, row._7)
    })


    if (result.count()==0){
      println("Points are not in any grids!!!!!!")
    }else{
      val result_schema = StructType(
        Array(
          StructField("poi_geohash", StringType),
          StructField("poi_address", StringType),
          StructField("poi_location", StringType),
          StructField("grid_location", StringType),
          StructField("grid_center_location", StringType),
          StructField("grid_district", StringType)
        )
      )
      val result_df = spark.createDataFrame(result, result_schema).repartition(1)
      result_df.write.csv(RESULT_PATH)
    }
  }
}
