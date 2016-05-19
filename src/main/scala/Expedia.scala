/**
  * Created by aissaelouafi on 09/05/2016.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Expedia {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Expedia-Kaggle-Competition").setMaster("local")
    val sc = new SparkContext(conf)

    // Disable INFO messages in spark console
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Upload CSV file
    val sqlContext = new SQLContext(sc)

    // Test data
    val test = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("../Expedia/test.csv")
    test.registerTempTable("test")
    val test2 = sqlContext.sql("SELECT * FROM test LIMIT 1000").toDF()
    test2.show(10)

    // Train data
    val train = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:9000/Expedia/SparkTrainData.csv")
    //train.registerTempTable("train")
    //val trainData = sqlContext.sql("SELECT * FROM train LIMIT 20000").toDF()
    //trainData.write.format("com.databricks.spark.csv").option("header","true").save("SparkTrainData.csv")
    //trainData.show(10)
    train.show(10)


    // Target
    val target = train.select("hotel_cluster")
    target.show(5)

  }

}
