package cn.kafka

import kafka.producer.Partitioner
import kafka.utils.{Utils, VerifiableProperties}

class CustomPartitioner(props: VerifiableProperties = null) extends Partitioner{
  override def partition(key: Any, numPartitions: Int) = {
    Utils.abs(key.hashCode) % numPartitions
  }
}
