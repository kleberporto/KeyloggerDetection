package KeyloggerDetection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Preprocessing {

  def renameColumns(df: DataFrame): DataFrame = {

    val renamedDf = df
    .withColumnRenamed("_c0", "Index")
    .withColumnRenamed("Flow ID", "FlowID")
    .withColumnRenamed(" Source IP", "SourceIP")
    .withColumnRenamed(" Source Port", "SourcePort")
    .withColumnRenamed(" Destination IP", "DestinationIP")
    .withColumnRenamed(" Destination Port", "DestinationPort")
    .withColumnRenamed(" Protocol", "Protocol")
    .withColumnRenamed(" Timestamp", "Timestamp")
    .withColumnRenamed(" Flow Duration", "FlowDuration")
    .withColumnRenamed(" Total Fwd Packets", "TotalFwdPackets")
    .withColumnRenamed(" Total Backward Packets", "TotalBackwardPackets")
    .withColumnRenamed("Total Length of Fwd Packets", "TotalLengthFwdPackets")
    .withColumnRenamed(" Total Length of Bwd Packets", "TotalLengthBwdPackets")
    .withColumnRenamed(" Fwd Packet Length Max", "FwdPacketLengthMax")
    .withColumnRenamed(" Fwd Packet Length Min", "FwdPacketLengthMin")
    .withColumnRenamed(" Fwd Packet Length Mean", "FwdPacketLengthMean")
    .withColumnRenamed(" Fwd Packet Length Std", "FwdPacketLengthStd")
    .withColumnRenamed("Bwd Packet Length Max", "BwdPacketLengthMax")
    .withColumnRenamed(" Bwd Packet Length Min", "BwdPacketLengthMin")
    .withColumnRenamed(" Bwd Packet Length Mean", "BwdPacketLengthMean")
    .withColumnRenamed(" Bwd Packet Length Std", "BwdPacketLengthStd")
    .withColumnRenamed("Flow Bytes/s", "FlowBytesPerSecond")
    .withColumnRenamed(" Flow Packets/s", "FlowPacketsPerSecond")
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
    .withColumnRenamed(" Fwd Header Length", "FwdHeaderLength")
    .withColumnRenamed(" Bwd Header Length", "BwdHeaderLength")
    .withColumnRenamed("Fwd Packets/s", "FwdPacketsPerSecond")
    .withColumnRenamed(" Bwd Packets/s", "BwdPacketsPerSecond")
    .withColumnRenamed(" Min Packet Length", "MinPacketLength")
    .withColumnRenamed(" Max Packet Length", "MaxPacketLength")
    .withColumnRenamed(" Packet Length Mean", "PacketLengthMean")
    .withColumnRenamed(" Packet Length Std", "PacketLengthStd")
    .withColumnRenamed(" Packet Length Variance", "PacketLengthVariance")
    .withColumnRenamed("FIN Flag Count", "FINFlagCount")
    .withColumnRenamed(" SYN Flag Count", "SYNFlagCount")
    .withColumnRenamed(" RST Flag Count", "RSTFlagCount")
    .withColumnRenamed(" PSH Flag Count", "PSHFlagCount")
    .withColumnRenamed(" ACK Flag Count", "ACKFlagCount")
    .withColumnRenamed(" URG Flag Count", "URGFlagCount")
    .withColumnRenamed(" CWE Flag Count", "CWEFlagCount")
    .withColumnRenamed(" ECE Flag Count", "ECEFlagCount")
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
    .withColumnRenamed("Class", "Class")

    renamedDf
  }

  def createInfectedCol(df: DataFrame): DataFrame = {
    df.withColumn("Infected", when(col("Class") === "Benign", lit(0))
        .otherwise(lit(1)))
  }

  def saveDfToParquet(cleanDf: DataFrame): Unit = {
    cleanDf.write.option("header", "true")
      .parquet("./data/processed/processed.parquet")
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

    withTargetColDf.printSchema()

    withTargetColDf.show(5)

    val finalDf = withTargetColDf

    println(s"Total lines ${finalDf.count}")

    val infectedCount: Long = finalDf.select("Infected").filter(col("Infected") === 1).count()

    println(s"Total of Infected rows: $infectedCount")

    saveDfToParquet(finalDf)

    spark.stop()
  }
}