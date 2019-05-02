package cse512

object HotzoneUtils {
  //Returns True if a point is in a rectangle, else false.
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectPoints = queryRectangle.split(",")
    val firstX = rectPoints(0).trim().toDouble
    val firstY = rectPoints(1).trim().toDouble
    val secondX = rectPoints(2).trim().toDouble
    val secondY = rectPoints(3).trim().toDouble

    val point = pointString.split(",")
    val pointX = point(0).trim().toDouble
    val pointY = point(1).trim().toDouble

    val largeX = math.max(firstX, secondX)
    val largeY = math.max(firstY, secondY)

    val smallX = math.min(firstX, secondX)
    val smallY = math.min(firstY, secondY)

    if (pointX >= smallX && pointY >= smallY && pointX <= largeX && pointY <= largeY) {
      return true
    } else {
      return false
    }
  }

  def ST_Within(pointString1:String, pointString2:String,  distance:Double): Boolean = {
    var point1 = pointString1.split(",")
    var point1X = point1(0).trim().toDouble
    var point1Y = point1(1).trim().toDouble

    var point2 = pointString2.split(",")
    var point2X = point2(0).trim().toDouble
    var point2Y = point2(1).trim().toDouble

    var originalDistance = math.sqrt(math.pow((point1X - point2X), 2)  + math.pow((point1Y - point2Y) , 2))
    if (originalDistance <= distance) {
      return true
    } else {
      return false
    }
  }
}
