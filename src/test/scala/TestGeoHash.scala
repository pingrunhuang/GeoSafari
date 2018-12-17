import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils
//import org.locationtech.geomesa.spark.jts.util.GeoHashUtils

/**
  * @ author Frank
  * @ date 2018/12/06 
  */
object TestGeoHash extends App {

//  The precision is how large the area you want your grid to match the poi
  val PRECISION = 12
  val geoFactory = new GeometryFactory()

  val point = geoFactory.createPoint(new Coordinate(121.82914948336449,31.48883550703687))
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

  val point1 = geoFactory.createPoint(new Coordinate(121.367369,31.103366))
  val left_down = new Coordinate(121.366733,31.103059)
  val right_down = new Coordinate(121.369435,31.103063)
  val right_up = new Coordinate(121.369436,31.105381)
  val left_up = new Coordinate(121.366733,31.105378)
  val grid1 = geoFactory.createPolygon(Array(
    left_down,
    right_down,
    right_up,
    left_up,
    left_down
  ))
  val center = geoFactory.createPoint(new Coordinate(121.36808448347708,31.10422000705052))
  println(grid1)
  println(point2)
  println(GeoHashUtils.encode(center, PRECISION))
  println(GeoHashUtils.encode(grid1, PRECISION))


  if (grid1.contains(point1)){
    println(s"$grid1 contains $point1")
  }else{
    println(s"$grid1 does not contain $point1")
  }

  val point2 = geoFactory.createPoint(new Coordinate(121.527017,31.256097))

  val grid2 = geoFactory.createPolygon(Array(
    new Coordinate(121.825066,30.931237),
    new Coordinate(121.827758,30.931231),
    new Coordinate(121.827758,30.93355),
    new Coordinate(121.825066,30.933555),
    new Coordinate(121.825066,30.931237)
  ))

  if (grid2.contains(point2)){
    println(s"$grid2 contains $point2")
  }else{
    println(s"$grid2 does not contain $point2")
  }

  val point3 = geoFactory.createPoint(new Coordinate(121.392350,31.240912))


  val grid3 = geoFactory.createPolygon(Array(
    new Coordinate(121.39106,31.239887),
    new Coordinate(121.39376,31.239889),
    new Coordinate(121.39376,31.242207),
    new Coordinate(121.39106,31.242206),
    new Coordinate(121.39106,31.239887)
  ))

  if (grid3.contains(point3)){
    println(s"$grid3 contains $point3")
  }else{
    println(s"$grid3 does not contain $point3")
  }





}
