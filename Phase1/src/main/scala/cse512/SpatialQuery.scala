package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def Contains(queryRect:String, pointString:String): Boolean = {
    var rectPoints = queryRect.split(",")
    var firstX = rectPoints(0).trim().toDouble
    var firstY = rectPoints(1).trim().toDouble
    var secondX = rectPoints(2).trim().toDouble
    var secondY = rectPoints(3).trim().toDouble

    var point = pointString.split(",")
    var pointX = point(0).trim().toDouble
    var pointY = point(1).trim().toDouble

    var largeX = math.max(firstX, secondX)
    var largeY = math.max(firstY, secondY)

    var smallX = math.min(firstX, secondX)
    var smallY = math.min(firstY, secondY)
    if (pointX >= smallX && pointY >= smallY && pointX <= largeX && pointY <= largeY) {
      return true
    } else {
      return false
    }
  }

  def Within(pointString1:String, pointString2:String,  distance:Double): Boolean = {
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
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
