// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ## Keylogger Detection Model
// MAGIC
// MAGIC Some difficulties with the ML lib: At first I was passing the Label as a feature
// MAGIC Setting the Threshold for the LogisticRegression

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, RandomForestClassifier, LinearSVC}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Creating constants so it's easier to use the columns

// COMMAND ----------

val PROTOCOL = "Protocol"
val FLOW_DURATION = "FlowDuration"
val TOTAL_FWD_PACKETS = "TotalFwdPackets"
val TOTAL_BWD_PACKETS = "TotalBackwardPackets"
val TOTAL_LENGTH_FWD_PACKETS = "TotalLengthFwdPackets"
val TOTAL_LENGTH_BWD_PACKETS = "TotalLengthBwdPackets"
val FWD_PACKET_LENGTH_MAX = "FwdPacketLengthMax"
val FWD_PACKET_LENGTH_MIN = "FwdPacketLengthMin"
val FWD_PACKET_LENGTH_MEAN = "FwdPacketLengthMean"
val FWD_PACKET_LENGTH_STD = "FwdPacketLengthStd"
val BWD_PACKET_LENGTH_MAX = "BwdPacketLengthMax"
val BWD_PACKET_LENGTH_MIN = "BwdPacketLengthMin"
val BWD_PACKET_LENGTH_MEAN = "BwdPacketLengthMean"
val BWD_PACKET_LENGTH_STD = "BwdPacketLengthStd"
val PACKET_LENGTH_STD = "PacketLengthStd"
val PACKET_LENGTH_VAR = "PacketLengthVariance"
val FLOW_BYTES_PER_SECOND = "FlowBytesPerSecond"
val FLOW_PACKETS_PER_SECOND = "FlowPacketsPerSecond"
val FWD_HEADER_LENGTH = "FwdHeaderLength"
val BWD_HEADER_LENGTH = "BwdHeaderLength"
val FWD_PACKETS_PER_SECOND = "FwdPacketsPerSecond"
val BWDPACKETS_PER_SECOND = "BwdPacketsPerSecond"
val MIN_PACKET_LENGTH = "MinPacketLength"
val MAX_PACKET_LENGTH = "MaxPacketLength"
val PACKET_LENGTH_MEAN = "PacketLengthMean"
val FIN_FLAG_COUNT = "FINFlagCount"
val SYN_FLAG_COUNT = "SYNFlagCount"
val RST_FLAG_COUNT = "RSTFlagCount"
val PSH_FLAG_COUNT = "PSHFlagCount"
val ACK_FLAG_COUNT = "ACKFlagCount"
val URG_FLAG_COUNT = "URGFlagCount"
val CWE_FLAG_COUNT = "CWEFlagCount"
val ECE_FLAG_COUNT = "ECEFlagCount"
val DOWN_UP_RATIO = "DownUpRatio"
val AVG_PACKET_SIZE = "AvgPacketSize"
val AVG_FWD_SEGMENT_SIZE = "AvgFwdSegmentSize"
val AVG_BWD_SEGMENT_SIZE = "AvgBwdSegmentSize"
val FWD_HEADER_LENGTH1 = "FwdHeaderLength1"
val FWD_AVG_BYTES_BY_BULK = "FwdAvgBytesByBulk"
val FWD_AVG_PACKETS_BY_BULK = "FwdAvgPacketsByBulk"
val FWD_AVG_BULK_RATE = "FwdAvgBulkRate"
val BWD_AVG_BYTES_BY_BULK = "BwdAvgBytesByBulk"
val BWD_AVG_PACKETS_BY_BULK = "BwdAvgPacketsByBulk"
val BWD_AVG_BULK_RATE = "BwdAvgBulkRate"
val SUB_FLOW_FWD_PACKETS = "SubflowFwdPackets"
val SUB_FLOW_FWD_BYTES = "SubflowFwdBytes"
val SUB_FLOW_BWD_PACKETS = "SubflowBwdPackets"
val SUB_FLOW_BWD_BYTES = "SubflowBwdBytes"
val INIT_WIN_BYTES_FORWARD = "InitWinBytesForward"
val INIT_WIN_BYTES_BACKWARD = "InitWinBytesBackward"
val ACT_DATA_PKT_FWD = "ActDataPktFwd"
val MIN_SEG_SIZE_FORWARD = "MinSegSizeForward"
val ACTIVE_MEAN = "ActiveMean"
val ACTIVE_STD = "ActiveStd"
val ACTIVE_MAX = "ActiveMax"
val ACTIVE_MIN = "ActiveMin"
val IDDLE_MEAN = "IddleMean"
val IDDLE_STD = "IddleStd"
val IDDLE_MAX = "IddleMax"
val IDDLE_MIN = "IddleMin"
val CLASS = "Class"
val PROTOCOL_VEC: String = PROTOCOL ++ "Vec"
val INFECTED = "Infected"

// COMMAND ----------

val pipelineSelectedCols: Array[String] = Array(
    FLOW_DURATION,
    TOTAL_FWD_PACKETS,
    TOTAL_BWD_PACKETS,
    TOTAL_LENGTH_FWD_PACKETS,
    TOTAL_LENGTH_BWD_PACKETS,
    FLOW_BYTES_PER_SECOND,
    FLOW_PACKETS_PER_SECOND,
    FIN_FLAG_COUNT,
    SYN_FLAG_COUNT,
    PSH_FLAG_COUNT,
    ACK_FLAG_COUNT,
    URG_FLAG_COUNT,
    ECE_FLAG_COUNT,
    DOWN_UP_RATIO,
    AVG_PACKET_SIZE,
    AVG_FWD_SEGMENT_SIZE,
    AVG_BWD_SEGMENT_SIZE,
    FWD_HEADER_LENGTH1,
    SUB_FLOW_FWD_PACKETS,
    SUB_FLOW_FWD_BYTES,
    SUB_FLOW_BWD_PACKETS,
    SUB_FLOW_BWD_BYTES,
    INIT_WIN_BYTES_FORWARD,
    INIT_WIN_BYTES_BACKWARD,
    ACT_DATA_PKT_FWD,
    MIN_SEG_SIZE_FORWARD,
    ACTIVE_MEAN,
    ACTIVE_STD,
    ACTIVE_MAX,
    ACTIVE_MIN,
    IDDLE_MEAN,
    IDDLE_STD,
    IDDLE_MAX,
    IDDLE_MIN,

    PROTOCOL_VEC
)

// COMMAND ----------

val pipelineSelectedColsTree: Array[String] = Array(
PROTOCOL,
FLOW_DURATION,
TOTAL_FWD_PACKETS,
TOTAL_BWD_PACKETS,
TOTAL_LENGTH_FWD_PACKETS,
TOTAL_LENGTH_BWD_PACKETS,
FLOW_BYTES_PER_SECOND,
FLOW_PACKETS_PER_SECOND,
FIN_FLAG_COUNT,
SYN_FLAG_COUNT,
PSH_FLAG_COUNT,
ACK_FLAG_COUNT,
URG_FLAG_COUNT,
ECE_FLAG_COUNT,
DOWN_UP_RATIO,
AVG_PACKET_SIZE,
AVG_FWD_SEGMENT_SIZE,
AVG_BWD_SEGMENT_SIZE,
FWD_HEADER_LENGTH1,
SUB_FLOW_FWD_PACKETS,
SUB_FLOW_FWD_BYTES,
SUB_FLOW_BWD_PACKETS,
SUB_FLOW_BWD_BYTES,
INIT_WIN_BYTES_FORWARD,
INIT_WIN_BYTES_BACKWARD,
ACT_DATA_PKT_FWD,
MIN_SEG_SIZE_FORWARD,
ACTIVE_MEAN,
ACTIVE_STD,
ACTIVE_MAX,
ACTIVE_MIN,
IDDLE_MEAN,
IDDLE_STD,
IDDLE_MAX,
IDDLE_MIN,
  )

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Definition of the DataSet Schema

// COMMAND ----------

val schema = StructType(List(
    StructField(PROTOCOL, DoubleType, nullable = true),
    StructField(FLOW_DURATION, DoubleType, nullable = true),
    StructField(TOTAL_FWD_PACKETS, DoubleType, nullable = true),
    StructField(TOTAL_BWD_PACKETS, DoubleType, nullable = true),
    StructField(TOTAL_LENGTH_FWD_PACKETS, DoubleType, nullable = true),
    StructField(TOTAL_LENGTH_BWD_PACKETS, DoubleType, nullable = true),
    StructField(FLOW_BYTES_PER_SECOND, DoubleType, nullable = true),
    StructField(FLOW_PACKETS_PER_SECOND, DoubleType, nullable = true),
    StructField(FIN_FLAG_COUNT, DoubleType, nullable = true),
    StructField(SYN_FLAG_COUNT, DoubleType, nullable = true),
    StructField(PSH_FLAG_COUNT, DoubleType, nullable = true),
    StructField(ACK_FLAG_COUNT, DoubleType, nullable = true),
    StructField(URG_FLAG_COUNT, DoubleType, nullable = true),
    StructField(DOWN_UP_RATIO, DoubleType, nullable = true),
    StructField(FWD_AVG_BYTES_BY_BULK, DoubleType, nullable = true),
    StructField(FWD_AVG_PACKETS_BY_BULK, DoubleType, nullable = true),
    StructField(FWD_AVG_BULK_RATE, DoubleType, nullable = true),
    StructField(BWD_AVG_BYTES_BY_BULK, DoubleType, nullable = true),
    StructField(BWD_AVG_PACKETS_BY_BULK, DoubleType, nullable = true),
    StructField(BWD_AVG_BULK_RATE, DoubleType, nullable = true),
    StructField(SUB_FLOW_FWD_PACKETS, DoubleType, nullable = true),
    StructField(SUB_FLOW_FWD_BYTES, DoubleType, nullable = true),
    StructField(SUB_FLOW_BWD_PACKETS, DoubleType, nullable = true),
    StructField(SUB_FLOW_BWD_BYTES, DoubleType, nullable = true),
    StructField(INIT_WIN_BYTES_FORWARD, DoubleType, nullable = true),
    StructField(INIT_WIN_BYTES_BACKWARD, DoubleType, nullable = true),
    StructField(ACT_DATA_PKT_FWD, DoubleType, nullable = true),
    StructField(MIN_SEG_SIZE_FORWARD, DoubleType, nullable = true),
    StructField(AVG_PACKET_SIZE, DoubleType, nullable = true),
    StructField(AVG_FWD_SEGMENT_SIZE, DoubleType, nullable = true),
    StructField(AVG_BWD_SEGMENT_SIZE, DoubleType, nullable = true),
    StructField(FWD_HEADER_LENGTH1, DoubleType, nullable = true),
    StructField(ACTIVE_MEAN, DoubleType, nullable = true),
    StructField(ACTIVE_STD, DoubleType, nullable = true),
    StructField(ACTIVE_MAX, DoubleType, nullable = true),
    StructField(ACTIVE_MIN, DoubleType, nullable = true),
    StructField(IDDLE_MEAN, DoubleType, nullable = true),
    StructField(IDDLE_STD, DoubleType, nullable = true),
    StructField(IDDLE_MAX, DoubleType, nullable = true),
    StructField(IDDLE_MIN, DoubleType, nullable = true),
    StructField(INFECTED, DoubleType,nullable =  true)
  ))

// COMMAND ----------

// Loading the Data
val file_location = "/FileStore/tables/preprocessed_data"
val rawDf = spark.read.option("header", "true").parquet(file_location)

// COMMAND ----------

val df = rawDf.na.drop()
display(df)

// COMMAND ----------

display(df.summary())

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Checking if the target class is balanced

// COMMAND ----------

display(df.groupBy(INFECTED).count())

// COMMAND ----------

display(df.select(FLOW_DURATION).where(col(INFECTED) === lit(1)))

// COMMAND ----------

// Checking the Average Packet size by Class. There isn't much information to get from this
display(df.groupBy(INFECTED).mean(AVG_PACKET_SIZE))

// COMMAND ----------

// Checking the Average Flow Duration by Class. There isn't much information to get from this

display(df.groupBy(INFECTED).mean(FLOW_DURATION))

// COMMAND ----------

/** Helper function to print the Metrics based on the predictions DataFrame */
def printConfusionMatrix(df: DataFrame): Unit = {
  val TP = df.filter( abs(col(INFECTED) - col("prediction")) < 0.5 && col(INFECTED) === 1).count
  val TN = df.filter( abs(col(INFECTED) - col("prediction")) < 0.5 && col(INFECTED) === 0).count
  val FP = df.filter( abs(col(INFECTED) - col("prediction")) > 0.5 && col(INFECTED) === 1).count
  val FN = df.filter( abs(col(INFECTED) - col("prediction")) > 0.5 && col(INFECTED) === 0).count

  println(s"True Positives: ${TP} \nTrue Negatives: ${TN}\nFalse Positives: ${FP}\nFalse Negatives: ${FN}\n")
  println(s"=== Confusion Matrix ====\n|   ${TP}    |     ${FP}    |\n|   ${FN}    |     ${TN}    |")

  try {
    val accuracy: Double = (TP + TN) / ((TP + TN + FP + FN) * 1.0)
    val precision: Double = TP / ((TP + FP) * 1.0)
    val recall: Double = TP / ( (TP + FN) * 1.0)
    val F1: Double = (precision * recall) / ((precision + recall) * 1.0)

    println(s"\nAccuracy = ${accuracy}")
    println(s"\nPrecision = ${precision}")
    println(s"\nRecall = ${recall}")
    println(s"\nF1 = ${F1}")
  } catch {
    case e: ArithmeticException => println("Division By Zero")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Logistic Regression

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Creating the train and test sets
// MAGIC
// MAGIC Creating the assembler to prepare the Data transformations and the LogisticRegression Model
// MAGIC
// MAGIC Adding everything to a Pipeline for better paral

// COMMAND ----------

// True Positives: 16543
// True Negatives: 10262
// False Positives: 4871
// False Negatives: 20751

// === Confusion Matrix ====
// |   16543    |     4871    |
// |   20751    |     10262    |

// Accuracy = 0.5112823545119881

// Precision = 0.7725319884187914

// Recall = 0.4435834182442216

// F1 = 0.2817844246099339

val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 42)

val protocolEncoder = new OneHotEncoder().setInputCols(Array(PROTOCOL)).setOutputCols(Array(PROTOCOL_VEC))

// Assemble everything together to be ("label","features") format
val assembler = new VectorAssembler().setInputCols(pipelineSelectedCols).setOutputCol("featuresUnscaled")

val scaler = new MinMaxScaler()
    .setInputCol("featuresUnscaled")
    .setOutputCol("features")
    .setMax(1)
    .setMin(0)

val model = new LogisticRegression().setLabelCol(INFECTED).setFeaturesCol("features").setMaxIter(100).setThreshold(0.4).setFamily("binomial")

val pipeline = new Pipeline().setStages(Array(protocolEncoder, assembler, scaler, model))

val fittedModel = pipeline.fit(training)

val predictions = fittedModel.transform(test)

printConfusionMatrix(predictions)

// COMMAND ----------


// True Positives: 4019
// True Negatives: 89547
// False Positives: 60073
// False Negatives: 3190

// === Confusion Matrix ====
// |   4019    |     60073    |
// |   3190    |     89547    |

// Accuracy = 0.5966115960696045

// Precision = 0.0627067340697747

// Recall = 0.04295363700489494

// F1 = 0.025491887503329997
val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 42)

val protocolEncoder = new OneHotEncoder().setInputCols(Array(PROTOCOL)).setOutputCols(Array(PROTOCOL_VEC))

// Assemble everything together to be ("label","features") format
val assembler = new VectorAssembler().setInputCols(pipelineSelectedCols).setOutputCol("featuresUnscaled")

val scaler = new MinMaxScaler()
    .setInputCol("featuresUnscaled")
    .setOutputCol("features")
    .setMax(1)
    .setMin(0)

val model = new LogisticRegression().setLabelCol(INFECTED).setFeaturesCol("features").setMaxIter(300).setThreshold(0.5).setFamily("binomial")

val pipeline = new Pipeline().setStages(Array(protocolEncoder, assembler, scaler, model))

val fittedModel = pipeline.fit(training)

val predictions = fittedModel.transform(test)

printConfusionMatrix(predictions)

// COMMAND ----------

// True Positives: 127
// True Negatives: 30915
// False Positives: 21287
// False Negatives: 98

// === Confusion Matrix ====
// |   127    |     21287    |
// |   98    |     30915    |

// Accuracy = 0.5920994907204303

// Precision = 0.005930699542355468

// Recall = 0.5644444444444444

// F1 = 0.005869032764915199

val Array(training, test) = df.withColumnRenamed(INFECTED, "label").randomSplit(Array(0.9, 0.1), seed = 42)

val protocolEncoder = new OneHotEncoder().setInputCols(Array(PROTOCOL)).setOutputCols(Array(PROTOCOL_VEC))

// Assemble everything together to be ("label","features") format
val assembler = new VectorAssembler().setInputCols(pipelineSelectedCols).setOutputCol("featuresUnscaled")

val scaler = new MinMaxScaler()
    .setInputCol("featuresUnscaled")
    .setOutputCol("features")
    .setMax(1)
    .setMin(0)

val model = new LogisticRegression().setFeaturesCol("features").setMaxIter(300).setThreshold(0.6).setFamily("binomial")

val pipeline = new Pipeline().setStages(Array(protocolEncoder, assembler, scaler, model))

// Fit the pipeline to training documents.
val fittedModel = pipeline.fit(training)

val predictions = fittedModel.transform(test)

printConfusionMatrix(predictions)

// COMMAND ----------

// True Positives: 16543
// True Negatives: 10262
// False Positives: 4871
// False Negatives: 20751

// === Confusion Matrix ====
// |   16543    |     4871    |
// |   20751    |     10262    |

// Accuracy = 0.5112823545119881

// Precision = 0.7725319884187914

// Recall = 0.4435834182442216

// F1 = 0.2817844246099339

val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 42)

val protocolEncoder = new OneHotEncoder().setInputCols(Array(PROTOCOL)).setOutputCols(Array(PROTOCOL_VEC))

// Assemble everything together to be ("label","features") format
val assembler = new VectorAssembler().setInputCols(pipelineSelectedCols).setOutputCol("featuresUnscaled")

val scaler = new MinMaxScaler()
    .setInputCol("featuresUnscaled")
    .setOutputCol("features")
    .setMax(1)
    .setMin(0)

val model = new LogisticRegression().setLabelCol(INFECTED).setFeaturesCol("features").setMaxIter(300).setThreshold(0.3).setFamily("binomial")

val pipeline = new Pipeline().setStages(Array(protocolEncoder, assembler, scaler, model))

// Fit the pipeline to training documents.
val fittedModel = pipeline.fit(training)

val predictions = fittedModel.transform(test)

printConfusionMatrix(predictions)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// Fit the pipeline to training documents.
val fittedModel = pipeline.fit(training)

// COMMAND ----------

val predictions = fittedModel.transform(test)

// COMMAND ----------

//display(predictions)

// COMMAND ----------

printConfusionMatrix(predictions)

// COMMAND ----------

// display(predictions.filter(col(INFECTED) =!= col("prediction")))

// COMMAND ----------

// fittedModel.write.overwrite.save("best-model")

// COMMAND ----------

val fittedLogReg = fittedModel.stages.last.asInstanceOf[org.apache.spark.ml.classification.LogisticRegressionModel]
println(fittedLogReg.binarySummary.accuracy)

// COMMAND ----------

val trainingSummary = fittedLogReg.summary

// Obtain the objective per iteration
val objectiveHistory = trainingSummary.objectiveHistory
println("objectiveHistory:")
objectiveHistory.foreach(println)

// for multiclass, we can inspect metrics on a per-label basis
println("False positive rate by label:")
trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("True positive rate by label:")
trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("Precision by label:")
trainingSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

println("Recall by label:")
trainingSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}


println("F-measure by label:")
trainingSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

val accuracy = trainingSummary.accuracy
val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
val truePositiveRate = trainingSummary.weightedTruePositiveRate
val fMeasure = trainingSummary.weightedFMeasure
val precision = trainingSummary.weightedPrecision
val recall = trainingSummary.weightedRecall
println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
  s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")

// COMMAND ----------

fittedLogReg.coefficients.toArray.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Random Forest Classifier

// COMMAND ----------

val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 42)

val assembler = new VectorAssembler().setInputCols(pipelineSelectedColsTree).setOutputCol("features")

val rfClassifier = new DecisionTreeClassifier().setLabelCol(INFECTED).setFeaturesCol("features").setMaxBins(40).setSeed(42)

val rfPipeline = new Pipeline().setStages(Array(assembler, rfClassifier))

// COMMAND ----------

val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol(INFECTED)
  .setMetricName("areaUnderROC")

val paramGrid = new ParamGridBuilder()
  .addGrid(rfClassifier.maxDepth, Array(6, 8, 10))
  .build()

val cv = new CrossValidator()
 .setEstimator(rfPipeline)
 .setEvaluator(evaluator)
 .setEstimatorParamMaps(paramGrid)
 .setNumFolds(5)
 .setSeed(42)


// COMMAND ----------

val cvModel = cv.fit(training)

// COMMAND ----------

cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

// COMMAND ----------

val rfPredictions = cvModel.transform(test)

// COMMAND ----------

printConfusionMatrix(rfPredictions)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------
