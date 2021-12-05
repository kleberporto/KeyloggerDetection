package KeyloggerDetection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import Constants._

object Preprocessing {

  def renameColumns(df: DataFrame): DataFrame = {

    val renamedDf = df
    .withColumnRenamed("_c0", "Index")
    .withColumnRenamed("Flow ID", "FlowID")
    .withColumnRenamed(" Source IP", "SourceIP")
    .withColumnRenamed(" Source Port", "SourcePort")
    .withColumnRenamed(" Destination IP", "DestinationIP")
    .withColumnRenamed(" Destination Port", "DestinationPort")
    .withColumnRenamed(" Protocol", PROTOCOL)
    .withColumnRenamed(" Timestamp", "Timestamp")
    .withColumnRenamed(" Flow Duration", FLOW_DURATION)
    .withColumnRenamed(" Total Fwd Packets", TOTAL_FWD_PACKETS)
    .withColumnRenamed(" Total Backward Packets", TOTAL_BWD_PACKETS)
    .withColumnRenamed("Total Length of Fwd Packets", TOTAL_LENGTH_FWD_PACKETS)
    .withColumnRenamed(" Total Length of Bwd Packets",TOTAL_LENGTH_BWD_PACKETS)
    .withColumnRenamed(" Fwd Packet Length Max", FWD_PACKET_LENGTH_MAX)
    .withColumnRenamed(" Fwd Packet Length Min", FWD_PACKET_LENGTH_MIN)
    .withColumnRenamed(" Fwd Packet Length Mean", FWD_PACKET_LENGTH_MEAN)
    .withColumnRenamed(" Fwd Packet Length Std", FWD_PACKET_LENGTH_STD)
    .withColumnRenamed("Bwd Packet Length Max", BWD_PACKET_LENGTH_MAX)
    .withColumnRenamed(" Bwd Packet Length Min", BWD_PACKET_LENGTH_MIN)
    .withColumnRenamed(" Bwd Packet Length Mean", BWD_PACKET_LENGTH_MEAN)
    .withColumnRenamed(" Bwd Packet Length Std", BWD_PACKET_LENGTH_STD)
    .withColumnRenamed("Flow Bytes/s", FLOW_BYTES_PER_SECOND)
    .withColumnRenamed(" Flow Packets/s", FLOW_PACKETS_PER_SECOND)
    .withColumnRenamed(" Flow IAT Mean", "FlowIATMean")
    .withColumnRenamed(" Flow IAT Std", "FlowIatStd")
    .withColumnRenamed(" Flow IAT Max", "FlowIatMax")
    .withColumnRenamed(" Flow IAT Min", "FlowIATMin")
    .withColumnRenamed("Fwd IAT Total", "FlowIATTotal")
    .withColumnRenamed(" Fwd IAT Mean", "FwdIATMean")
    .withColumnRenamed(" Fwd IAT Std", "FwdIATStd")
    .withColumnRenamed(" Fwd IAT Max", "FwdIatMax")
    .withColumnRenamed(" Fwd IAT Min", "FwdIATMin")
    .withColumnRenamed("Bwd IAT Total", "BwdIATTotal")
    .withColumnRenamed(" Bwd IAT Mean", "BwdIATMean")
    .withColumnRenamed(" Bwd IAT Std", "BwdIATStd")
    .withColumnRenamed(" Bwd IAT Max", "BwdIATMax")
    .withColumnRenamed(" Bwd IAT Min", "BwdIATMin")
    .withColumnRenamed("Fwd PSH Flags", "FwdPSHFlags")
    .withColumnRenamed(" Bwd PSH Flags", "BwdPSHFlags")
    .withColumnRenamed(" Fwd URG Flags", "FwdURGFlags")
    .withColumnRenamed(" Bwd URG Flags", "BwdURGFlags")
    .withColumnRenamed(" Fwd Header Length", FWD_HEADER_LENGTH)
    .withColumnRenamed(" Bwd Header Length", BWD_HEADER_LENGTH)
    .withColumnRenamed("Fwd Packets/s", FWD_PACKETS_PER_SECOND)
    .withColumnRenamed(" Bwd Packets/s", BWDPACKETS_PER_SECOND)
    .withColumnRenamed(" Min Packet Length", MIN_PACKET_LENGTH)
    .withColumnRenamed(" Max Packet Length", MAX_PACKET_LENGTH)
    .withColumnRenamed(" Packet Length Mean", PACKET_LENGTH_MEAN)
    .withColumnRenamed(" Packet Length Std", PACKET_LENGTH_STD)
    .withColumnRenamed(" Packet Length Variance", PACKET_LENGTH_VAR)
    .withColumnRenamed("FIN Flag Count", FIN_FLAG_COUNT)
    .withColumnRenamed(" SYN Flag Count", SYN_FLAG_COUNT)
    .withColumnRenamed(" RST Flag Count", RST_FLAG_COUNT)
    .withColumnRenamed(" PSH Flag Count", PSH_FLAG_COUNT)
    .withColumnRenamed(" ACK Flag Count", ACK_FLAG_COUNT)
    .withColumnRenamed(" URG Flag Count", URG_FLAG_COUNT)
    .withColumnRenamed(" CWE Flag Count", CWE_FLAG_COUNT)
    .withColumnRenamed(" ECE Flag Count", ECE_FLAG_COUNT)
    .withColumnRenamed(" Down/Up Ratio", DOWN_UP_RATIO)
    .withColumnRenamed(" Average Packet Size", AVG_PACKET_SIZE)
    .withColumnRenamed(" Avg Fwd Segment Size", AVG_FWD_SEGMENT_SIZE)
    .withColumnRenamed(" Avg Bwd Segment Size", AVG_BWD_SEGMENT_SIZE)
    .withColumnRenamed(" Fwd Header Length.1", FWD_HEADER_LENGTH1)
    .withColumnRenamed("Fwd Avg Bytes/Bulk", FWD_AVG_BYTES_BY_BULK)
    .withColumnRenamed(" Fwd Avg Packets/Bulk", FWD_AVG_PACKETS_BY_BULK)
    .withColumnRenamed(" Fwd Avg Bulk Rate", FWD_AVG_BULK_RATE)
    .withColumnRenamed(" Bwd Avg Bytes/Bulk", BWD_AVG_BYTES_BY_BULK)
    .withColumnRenamed(" Bwd Avg Packets/Bulk", BWD_AVG_PACKETS_BY_BULK)
    .withColumnRenamed("Bwd Avg Bulk Rate", BWD_AVG_BULK_RATE)
    .withColumnRenamed("Subflow Fwd Packets", SUB_FLOW_FWD_PACKETS)
    .withColumnRenamed(" Subflow Fwd Bytes", SUB_FLOW_FWD_BYTES)
    .withColumnRenamed(" Subflow Bwd Packets", SUB_FLOW_BWD_PACKETS)
    .withColumnRenamed(" Subflow Bwd Bytes", SUB_FLOW_BWD_BYTES)
    .withColumnRenamed("Init_Win_bytes_forward", INIT_WIN_BYTES_FORWARD)
    .withColumnRenamed(" Init_Win_bytes_backward", INIT_WIN_BYTES_BACKWARD)
    .withColumnRenamed(" act_data_pkt_fwd", ACT_DATA_PKT_FWD)
    .withColumnRenamed(" min_seg_size_forward", MIN_SEG_SIZE_FORWARD)
    .withColumnRenamed("Active Mean", ACTIVE_MEAN)
    .withColumnRenamed(" Active Std", ACTIVE_STD)
    .withColumnRenamed(" Active Max", ACTIVE_MAX)
    .withColumnRenamed(" Active Min", ACTIVE_MIN)
    .withColumnRenamed("Idle Mean", IDDLE_MEAN)
    .withColumnRenamed(" Idle Std", IDDLE_STD)
    .withColumnRenamed(" Idle Max", IDDLE_MAX)
    .withColumnRenamed(" Idle Min", IDDLE_MIN)
    .withColumnRenamed("Class", CLASS)
    .withColumn(INFECTED, col(CLASS))

    renamedDf
  }

  // Filtering protocols not in the valid list
  def filterProtocolOutliers(df: DataFrame): DataFrame = {
    val filteredDf = df.filter(col(PROTOCOL).isin(validProtocolsList:_*))
    filteredDf
  }

  def createInfectedCol(df: DataFrame): DataFrame = {
    df.withColumn(INFECTED, when(col(INFECTED) === "Benign", lit(0))
        .otherwise(lit(1)))
  }

  def saveDfToParquet(cleanDf: DataFrame): Unit = {
    cleanDf.write.option("header", "true")
      .mode(SaveMode.Overwrite)
      .parquet(PROCESSED_DATA_PATH)
  }

  def selectColumns(allColumnsDf: DataFrame): DataFrame = {
    val retDf = allColumnsDf.select(usedCols:_*)
    retDf
  }

  def processDf(df: DataFrame): DataFrame = {
    val processedDf = filterProtocolOutliers(df)
    processedDf
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("KeyloggerDetection")
      .master("local[*]")
      .getOrCreate()

    val rawDf = spark.read.option("header", value = true)
      .option("InferSchema", value = true)
      .csv("./data/raw/Keylogger_Detection.csv")

    val renamedDf = renameColumns(rawDf)
    val withTargetColDf = createInfectedCol(renamedDf)
    val selectedColumnsDf = selectColumns(withTargetColDf)
    val processedDf = processDf(selectedColumnsDf)
    processedDf.printSchema()

    processedDf.show(5)

    val finalDf = processedDf

    println(s"Total lines ${finalDf.count}")

    val infectedCount: Long = finalDf.select(INFECTED).filter(col(INFECTED) === 1).count()

    println(s"Total of Infected rows: $infectedCount")

    //finalDf.summary().show(false)

    saveDfToParquet(finalDf)

    spark.stop()
  }
}