/**
  * Created by aissaelouafi on 09/05/2016.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

object Expedia {
  /**
    *
    * @param args
    */

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Expedia-Kaggle-Competition").setMaster("local")
    val sc = new SparkContext(conf)

    // Disable INFO messages in spark console
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Upload CSV file
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Test data
    val test = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/test.csv")
    //test.registerTempTable("test")
    //val subTest = sqlContext.sql("SELECT * FROM test LIMIT 10000").toDF()
    //subTest.write.format("com.databricks.spark.csv").option("header","true").save("test.csv")


    // Train data
    var train = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/SparkTrainData.csv").toDF()


    train = train
      .withColumn("date_time", $"date_time".cast("timestamp"))  // cast to timestamp
      .withColumn("year", year($"date_time")) // add year column
      .withColumn("month",month($"date_time")) // add month colmun
      .withColumn("month_day",dayofmonth($"date_time"))
      .withColumn("year_day",dayofyear($"date_time"))
      .withColumn("srch_ci_date",$"srch_ci".cast("timestamp"))
      .withColumn("srch_co_date",$"srch_co".cast("timestamp"))
      .withColumn("nights_booked",dayofyear($"srch_co")-dayofyear($"srch_ci"))



    // Target
    val target = train.select("hotel_cluster")
    train.printSchema()
    train.show()
  }

}
