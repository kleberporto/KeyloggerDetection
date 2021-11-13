package KeyloggerDetection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler

object KeyloggerDetection {

  case class RegressionSchema(label: Double, features_raw: Double)

//  def loadDf(): DataFrame = {
//    spark.read.parquet("./data/processed")
//  }

  def train(trainDF: DataFrame): Unit = {

  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("KeyloggerDetection")
      .master("local[*]")
      .getOrCreate()


    spark.stop()

  }
}
