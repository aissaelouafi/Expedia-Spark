/**
  * Created by aissaelouafi on 09/05/2016.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors,SparseVector,DenseVector}

object Expedia {
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
    var train = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/SparkTrainData.csv")


    // Destinations data
    var destinations = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/destinations.csv")

    // Generate train features
    train = train
      .withColumn("date_time", $"date_time".cast("timestamp"))  // cast to timestamp
      .withColumn("year", year($"date_time")) // add year column

      // srch_ci date attributs :
      .withColumn("srch_ci",$"srch_ci".cast("timestamp"))
      .withColumn("ci_dayofmonth",dayofmonth($"srch_ci"))
      .withColumn("ci_month",month($"srch_ci"))
      .withColumn("ci_dayofyear",dayofyear($"srch_ci"))

      // srch_ci date attributs :
      .withColumn("srch_co",$"srch_co".cast("timestamp"))
      .withColumn("co_dayofmonth",dayofmonth($"srch_co"))
      .withColumn("co_month",month($"srch_co"))
      .withColumn("co_dayofyear",dayofyear($"srch_co"))

      // nights booked
      .withColumn("nights_booked",dayofyear($"srch_co")-dayofyear($"srch_ci"))

    train = train.na.fill(0)


    // Construct a RDD[LabeledPoint] from the dataframe
    val ignored = List("hotel_cluster")
    val featInd = train.columns.diff(ignored).map(train.columns.indexOf(_))
    val targetIndice = train.columns.indexOf("hotel_cluster")

    train.rdd.map(r => LabeledPoint(r.getDouble(targetIndice), // Get the hotel_cluster value
      Vectors.dense(featInd.map(r.getDouble(_)).toArray) // Map feature indice to values
    ))


    // Apply PCA to destinations data in order to reduce the dimension
    val destinationRDD = destinations.rdd
    val testt:LabeledPoint = LabeledPoint(1.0, Vectors.dense(1.0,0.0,2.0))
    //val pca = new PCA(2).fit(testt.map(_.features))
    println(testt.label.toString())

    // Target
    val target = train.select("hotel_cluster")
    train.printSchema()
    train.show()
    println(train.stat.corr("posa_continent","user_location_country"))
  }

}
