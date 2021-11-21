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
    .withColumnRenamed(" Fwd Packet Length Max", "FwdPacketLengthMax")
    .withColumnRenamed(" Fwd Packet Length Min", "FwdPacketLengthMin")
    .withColumnRenamed(" Fwd Packet Length Mean", "FwdPacketLengthMean")
    .withColumnRenamed(" Fwd Packet Length Std", "FwdPacketLengthStd")
    .withColumnRenamed("Bwd Packet Length Max", "BwdPacketLengthMax")
    .withColumnRenamed(" Bwd Packet Length Min", "BwdPacketLengthMin")
    .withColumnRenamed(" Bwd Packet Length Mean", "BwdPacketLengthMean")
    .withColumnRenamed(" Bwd Packet Length Std", "BwdPacketLengthStd")
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
    .withColumnRenamed(" Packet Length Std", "PacketLengthStd")
    .withColumnRenamed(" Packet Length Variance", "PacketLengthVariance")
    .withColumnRenamed("FIN Flag Count", FIN_FLAG_COUNT)
    .withColumnRenamed(" SYN Flag Count", SYN_FLAG_COUNT)
    .withColumnRenamed(" RST Flag Count", RST_FLAG_COUNT)
    .withColumnRenamed(" PSH Flag Count", PSH_FLAG_COUNT)
    .withColumnRenamed(" ACK Flag Count", ACK_FLAG_COUNT)
    .withColumnRenamed(" URG Flag Count", URG_FLAG_COUNT)
    .withColumnRenamed(" CWE Flag Count", CWE_FLAG_COUNT)
    .withColumnRenamed(" ECE Flag Count", ECE_FLAG_COUNT)
    .withColumnRenamed(" Down/Up Ratio", "DownUpRatio")
    .withColumnRenamed(" Average Packet Size", "AvgPacketSize")
    .withColumnRenamed(" Avg Fwd Segment Size", "AvgFwdSegmentSize")
    .withColumnRenamed(" Avg Bwd Segment Size", "AvgBwdSegmentSize")
    .withColumnRenamed(" Fwd Header Length.1", "FwdHeaderLength1")
    .withColumnRenamed("Fwd Avg Bytes/Bulk", "FwdAvgBytesByBulk")
    .withColumnRenamed(" Fwd Avg Packets/Bulk", "FwdAvgPacketsByBulk")
    .withColumnRenamed(" Fwd Avg Bulk Rate", "FwdAvgBulkRate")
    .withColumnRenamed(" Bwd Avg Bytes/Bulk", "BwdAvgBytesByBulk")
    .withColumnRenamed(" Bwd Avg Packets/Bulk", "BwdAvgPacketsByBulk")
    .withColumnRenamed("Bwd Avg Bulk Rate", "BwdAvgBulkRate")
    .withColumnRenamed("Subflow Fwd Packets", "SubflowFwdPackets")
    .withColumnRenamed(" Subflow Fwd Bytes", "SubflowFwdBytes")
    .withColumnRenamed(" Subflow Bwd Packets", "SubflowBwdPackets")
    .withColumnRenamed(" Subflow Bwd Bytes", "SubflowBwdBytes")
    .withColumnRenamed("Init_Win_bytes_forward", "InitWinBytesForward")
    .withColumnRenamed(" Init_Win_bytes_backward", "InitWinBytesBackward")
    .withColumnRenamed(" act_data_pkt_fwd", "ActDataPktFwd")
    .withColumnRenamed(" min_seg_size_forward", "MinSegSizeForward")
    .withColumnRenamed("Active Mean", "ActiveMean")
    .withColumnRenamed(" Active Std", "ActiveStd")
    .withColumnRenamed(" Active Max", "ActiveMax")
    .withColumnRenamed(" Active Min", "ActiveMin")
    .withColumnRenamed("Idle Mean", "IddleMean")
    .withColumnRenamed(" Idle Std", "IddleStd")
    .withColumnRenamed(" Idle Max", "IddleMax")
    .withColumnRenamed(" Idle Min", "IddleMin")
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

    saveDfToParquet(finalDf)

    spark.stop()
  }
}