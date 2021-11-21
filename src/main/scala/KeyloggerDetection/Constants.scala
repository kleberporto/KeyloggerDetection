package KeyloggerDetection

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}


object Constants {

  val PROCESSED_DATA_PATH = "./data/processed/processed.parquet"


  val PROTOCOL = "Protocol"
  val FLOW_DURATION = "FlowDuration"
  val TOTAL_FWD_PACKETS = "TotalFwdPackets"
  val TOTAL_BWD_PACKETS = "TotalBackwardPackets"
  val TOTAL_LENGTH_FWD_PACKETS = "TotalLengthFwdPackets"
  val TOTAL_LENGTH_BWD_PACKETS = "TotalLengthBwdPackets"
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
    col(RST_FLAG_COUNT).cast(IntegerType).as(RST_FLAG_COUNT),
    col(PSH_FLAG_COUNT).cast(IntegerType).as(PSH_FLAG_COUNT),
    col(ACK_FLAG_COUNT).cast(IntegerType).as(ACK_FLAG_COUNT),
    col(URG_FLAG_COUNT).cast(IntegerType).as(URG_FLAG_COUNT),
    col(CWE_FLAG_COUNT).cast(IntegerType).as(CWE_FLAG_COUNT),
    col(ECE_FLAG_COUNT).cast(IntegerType).as(ECE_FLAG_COUNT),
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
      RST_FLAG_COUNT,
      PSH_FLAG_COUNT,
      ACK_FLAG_COUNT,
      URG_FLAG_COUNT,
      CWE_FLAG_COUNT,
      ECE_FLAG_COUNT,
      PROTOCOL_VEC,
      INFECTED
  )
}
