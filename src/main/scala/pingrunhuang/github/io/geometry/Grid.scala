package pingrunhuang.github.io.geometry

/**
  * @ author Frank Huang
  * @ date 2018/12/15 
  */
case class Grid(grid_code:String,
                up_left:Location,
                down_left:Location,
                down_right:Location,
                up_right:Location,
                center:Location,
                district_code:Int) extends IGeo
