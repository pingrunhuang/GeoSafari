import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils

/**
  * @ author Frank Huang (runping@shanshu.ai)
  * @ date 2018/12/06 
  */
object TestGeoHash extends App {

//  The precision is how large the area you want your grid to match the poi
  val PRECISION = 1
  val geoFactory = new GeometryFactory()

  val GRIDS_PATH = "PATH_OF_YOUR_GRIDS_DATA"
  val POI_PATH = "POINTS_OF_YOUR_POI_DATA"


  val point = geoFactory.createPoint(new Coordinate(3.4, 5.6))
  println(point)
  val hashed_grid = GeoHashUtils.encode(point, PRECISION)
  println(hashed_grid)
  val decoded_grid = GeoHashUtils.decode(hashed_grid, PRECISION)
  println(decoded_grid.getCentroid)
  if (decoded_grid.contains(point)){
    println("It work!!!!")
  }else{
    println("It does not work")
  }

  val spark = SparkSession.builder().appName("master").master("local[4]").getOrCreate().withJTS

  val grid_columns = Seq("left_down", "left_down", "right_down", "right_up", "left_up", "center", "address")
  val poi_columns = Seq("locationx", "locationy", "addr")

  val grid_portrait_df = spark.read.option("sep", "|").option("header","true").csv(GRIDS_PATH).select(
    "left_down", "left_down", "right_down", "right_up", "left_up", "center", "address"
  )
  val poi_df = spark.read.option("header", "true").csv(POI_PATH).select(
    "locationx", "locationy", "addr"
  )

  val poi1 = poi_df.withColumn("point", st_makePoint(poi_df("locationx"), poi_df("locationy")))
  val poi =  poi1.withColumn("point_geohash", st_geoHash(poi1("point"), PRECISION))
  poi.cache()
  poi.show(5)
//  val grid_portrait_rdd = grid_portrait_df.withColumn("grid_geohash", st_geoHash(st_pointFromText(grid_portrait_df("center")), PRECISION)).rdd
  val grid_portrait_rdd = grid_portrait_df.rdd.map(row=>{
    val centerx = row.getAs[String]("center").split(",")(0).toDouble
    val centery = row.getAs[String]("center").split(",")(1).toDouble
    val center = geoFactory.createPoint(new Coordinate(centerx, centery))
    val geohash = GeoHashUtils.encode(center, PRECISION)
    (geohash, row)
  })
  grid_portrait_rdd.take(5)

  val grids_dict = grid_portrait_rdd.map(row=>(row._1, row._2)).groupByKey() .collectAsMap()
  val grids_dict_bc = spark.sparkContext.broadcast[scala.collection.Map[String, Iterable[Row]]](grids_dict)

  val result = poi.rdd.map(row=>{
    val hash_value = row.getAs[String]("point_geohash")
    val poi_x = row.getAs[String]("locationx").toDouble
    val poi_y = row.getAs[String]("locationy").toDouble
    val poi_point = geoFactory.createPoint(new Coordinate(poi_x, poi_y))
    val match_grids = grids_dict_bc.value.get(hash_value)

    if (match_grids.isDefined){
      val matched_grid = match_grids.get.map(g=>{
        val left_up = new Coordinate(g.getAs[String]("left_up").split(",")(0).toDouble, g.getAs[String]("left_up").split(",")(1).toDouble)
        val left_down = new Coordinate(g.getAs[String]("left_down").split(",")(0).toDouble, g.getAs[String]("left_down").split(",")(1).toDouble)
        val right_up = new Coordinate(g.getAs[String]("right_up").split(",")(0).toDouble, g.getAs[String]("right_up").split(",")(1).toDouble)
        val right_down = new Coordinate(g.getAs[String]("right_down").split(",")(0).toDouble, g.getAs[String]("right_down").split(",")(1).toDouble)
        val polygon = geoFactory.createPolygon(Array(left_up, left_down, right_down, right_up, left_up))
        (polygon, g.getAs[String]("center"), g.getAs[String]("address"))
      }).filter(bar=>bar._1.contains(poi_point)).head

      (row, true, matched_grid._1.toString, matched_grid._2, matched_grid._3)
    }else{
      (row, false, "", "", "")
    }
  }).filter(row=>row._2).map(row=>{
    Row(row._1.getAs[String]("point_geohash"), row._1.getAs[String]("addr"), row._1.getAs[String]("point"), row._3, row._4, row._5)
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
        StructField("grid_address", StringType)
      )
    )
    val result_df = spark.createDataFrame(result, result_schema).repartition(1)
    result_df.write.csv("match_result.csv")
  }

}
