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
    val test = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("../Expedia/test.csv")
    test.registerTempTable("test")
    val subTest = sqlContext.sql("SELECT * FROM test LIMIT 1000").toDF()


    // Train data
    var train = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/SparkTrainData.csv").toDF()

    train = train
      .withColumn("date_time", $"date_time".cast("timestamp"))  // cast to timestamp
      .withColumn("year", year($"date_time")) // add year column
      .withColumn("month",month($"date_time")) // add month colmun
      .withColumn("month_day",dayofmonth($"date_time"))
      .withColumn("year_day",dayofyear($"date_time"))


    // Using a map on rows
    //train.map {
    //  case Row(col1: String, col2: Int, col3: Int) => (col1, col2, col3, DateTime.parse(col1, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).getYear)
    //}.toDF("date_time", "site_name", "posa_continent", "year").show()

    
    // Target
    val target = train.select("hotel_cluster")
    train.printSchema()
    train.show()
  }

}
