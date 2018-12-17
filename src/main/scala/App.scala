import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.spark.jts._
import pingrunhuang.github.io.GridFacility

/**
  * @ author Frank Huang
  * @ date 2018/12/06 
  */

object App {
  private val SQL_URL     = "jdbc:postgresql://52.83.177.118:5432/store_location"
  private val SQL_USER    = "bdatadev"
  private val SQL_PASS    = "Shanshu.1204"
  private val SQL_DRIVER  = "org.postgresql.Driver"
  private val GRID_TABLE  = "grid_info"
  private val POI_TABLE   = "poi_info"


  private val spark     = SparkSession.builder().
    appName("Geo-Safari")
    .master("local[4]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    .getOrCreate().withJTS

  def main(args: Array[String]): Unit = {
    val precision:Int = args(0).toInt

    val poi_df = spark.read.format("jdbc")
      .option("url", SQL_URL)
      .option("user", SQL_USER)
      .option("password", SQL_PASS)
      .option("driver", SQL_DRIVER)
      .option("dbtable", POI_TABLE).load()
    poi_df.show(5)

    val grid_portrait_df = spark.read.format("jdbc")
      .option("url", SQL_URL)
      .option("user", SQL_USER)
      .option("password", SQL_PASS)
      .option("driver", SQL_DRIVER)
      .option("dbtable", GRID_TABLE).load()
    grid_portrait_df.show(5)

    val grid_facility = new GridFacility(spark)
//    grid_facility.pointGridMatch(grid_portrait_df, poi_df, precision)
  }
}
