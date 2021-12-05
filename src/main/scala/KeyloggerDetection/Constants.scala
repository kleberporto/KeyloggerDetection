package KeyloggerDetection

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}


object Constants {

  val PROCESSED_DATA_PATH = "./data/processed/processed.parquet"


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

  val usedCols: Seq[Column] = Seq(
    col(PROTOCOL).cast(IntegerType).as(PROTOCOL),
    col(FLOW_DURATION).cast(DoubleType).as(FLOW_DURATION),
    col(TOTAL_FWD_PACKETS).cast(IntegerType).as(TOTAL_FWD_PACKETS),
    col(TOTAL_BWD_PACKETS).cast(IntegerType).as(TOTAL_BWD_PACKETS),
    col(TOTAL_LENGTH_FWD_PACKETS).cast(IntegerType).as(TOTAL_LENGTH_FWD_PACKETS),
    col(TOTAL_LENGTH_BWD_PACKETS).cast(IntegerType).as(TOTAL_LENGTH_BWD_PACKETS),
    col(FLOW_BYTES_PER_SECOND).cast(DoubleType).as(FLOW_BYTES_PER_SECOND),
    col(FLOW_PACKETS_PER_SECOND).cast(DoubleType).as(FLOW_PACKETS_PER_SECOND),
    col(FIN_FLAG_COUNT).cast(IntegerType).as(FIN_FLAG_COUNT),
    col(SYN_FLAG_COUNT).cast(IntegerType).as(SYN_FLAG_COUNT),
    col(PSH_FLAG_COUNT).cast(IntegerType).as(PSH_FLAG_COUNT),
    col(ACK_FLAG_COUNT).cast(IntegerType).as(ACK_FLAG_COUNT),
    col(URG_FLAG_COUNT).cast(IntegerType).as(URG_FLAG_COUNT),
    col(ECE_FLAG_COUNT).cast(IntegerType).as(ECE_FLAG_COUNT),
    col(FWD_AVG_BYTES_BY_BULK).cast(DoubleType).as(FWD_HEADER_LENGTH1),
    col(FWD_AVG_PACKETS_BY_BULK).cast(DoubleType).as(FWD_AVG_BYTES_BY_BULK),
    col(FWD_AVG_BULK_RATE).cast(DoubleType).as(FWD_AVG_PACKETS_BY_BULK),
    col(BWD_AVG_BYTES_BY_BULK).cast(DoubleType).as(FWD_AVG_BULK_RATE),
    col(BWD_AVG_PACKETS_BY_BULK).cast(DoubleType).as(BWD_AVG_BYTES_BY_BULK),
    col(BWD_AVG_BULK_RATE).cast(DoubleType).as(BWD_AVG_PACKETS_BY_BULK),
    col(DOWN_UP_RATIO).cast(DoubleType).as(DOWN_UP_RATIO),
    col(AVG_PACKET_SIZE).cast(DoubleType).as(AVG_PACKET_SIZE),
    col(AVG_FWD_SEGMENT_SIZE).cast(DoubleType).as(AVG_FWD_SEGMENT_SIZE),
    col(AVG_BWD_SEGMENT_SIZE).cast(DoubleType).as(AVG_BWD_SEGMENT_SIZE),
    col(FWD_HEADER_LENGTH1).cast(DoubleType).as(FWD_HEADER_LENGTH1),
    col(SUB_FLOW_FWD_PACKETS).cast(DoubleType).as(SUB_FLOW_FWD_PACKETS),
    col(SUB_FLOW_FWD_BYTES).cast(DoubleType).as(SUB_FLOW_FWD_BYTES),
    col(SUB_FLOW_BWD_PACKETS).cast(DoubleType).as(SUB_FLOW_BWD_PACKETS),
    col(SUB_FLOW_BWD_BYTES).cast(DoubleType).as(SUB_FLOW_BWD_BYTES),
    col(INIT_WIN_BYTES_FORWARD).cast(DoubleType).as(INIT_WIN_BYTES_FORWARD),
    col(INIT_WIN_BYTES_BACKWARD).cast(DoubleType).as(INIT_WIN_BYTES_BACKWARD),
    col(ACT_DATA_PKT_FWD).cast(DoubleType).as(ACT_DATA_PKT_FWD),
    col(MIN_SEG_SIZE_FORWARD).cast(DoubleType).as(MIN_SEG_SIZE_FORWARD),
    col(ACTIVE_MEAN).cast(DoubleType).as(ACTIVE_MEAN),
    col(ACTIVE_STD).cast(DoubleType).as(ACTIVE_STD),
    col(ACTIVE_MAX).cast(DoubleType).as(ACTIVE_MAX),
    col(ACTIVE_MIN).cast(DoubleType).as(ACTIVE_MIN),
    col(IDDLE_MEAN).cast(DoubleType).as(IDDLE_MEAN),
    col(IDDLE_STD).cast(DoubleType).as(IDDLE_STD),
    col(IDDLE_MAX).cast(DoubleType).as(IDDLE_MAX),
    col(IDDLE_MIN).cast(DoubleType).as(IDDLE_MIN),
    col(INFECTED).cast(IntegerType).as(INFECTED),
  )

  val validProtocolsList: Seq[Int] = Seq(0, 6, 17)

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
      PROTOCOL_VEC,
      INFECTED
  )

  val inputSchema: StructType = StructType(List(
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
    StructField(SUB_FLOW_FWD_PACKETS, DoubleType, nullable = true),
    StructField(SUB_FLOW_FWD_BYTES, DoubleType, nullable = true),
    StructField(SUB_FLOW_BWD_PACKETS, DoubleType, nullable = true),
    StructField(SUB_FLOW_BWD_BYTES, DoubleType, nullable = true),
    StructField(MIN_SEG_SIZE_FORWARD, DoubleType, nullable = true),
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

}
