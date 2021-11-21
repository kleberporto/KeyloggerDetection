package KeyloggerDetection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import Constants._


object KeyloggerDetection {

  case class RegressionSchema(label: Double, features_raw: Double)

  def loadDf(spark: SparkSession): DataFrame = {
    val df = spark.read
      .option("InferSchema", value=true)
      .option("header", "true")
      .parquet("./data/processed/processed.parquet")

    df.printSchema()

    df
  }

  def getModel(modelName: String): PipelineStage ={
    val model = {
      modelName match {
        case "LogisticRegression" => new LogisticRegression().setLabelCol(INFECTED)
        case "DecisionTreeClassifier" => new DecisionTreeClassifier().setLabelCol(INFECTED)
      }
    }
    model
  }

  def createPipeline(modelName: String): Pipeline = {

    val protocolEncoder = new OneHotEncoder()
      .setInputCols(Array(PROTOCOL))
      .setOutputCols(Array(PROTOCOL_VEC))

    // Assemble everything together to be ("label","features") format
    val assembler = new VectorAssembler()
      .setInputCols(pipelineSelectedCols)
      .setOutputCol("features")

    val model = getModel(modelName)

    val pipeline = new Pipeline()
                          .setStages(Array(protocolEncoder, assembler, model))

    pipeline
  }

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

    val processedDf = loadDf(spark)

    val Array(training, test) = processedDf.randomSplit(Array(0.7, 0.3), seed = 42)

    val pipeline = createPipeline("LogisticRegression")

    // Fit the pipeline to training documents.
    val fittedModel = pipeline.fit(training)

    val results = fittedModel.transform(test)

    results.show()

    spark.stop()

  }
}
