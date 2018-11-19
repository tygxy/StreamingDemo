package com.bupt.spark.APP

import java.util.Properties
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject, TypeReference}
import com.bupt.Hbase.HBaseUtils
import com.bupt.spark.Bean.{Output, Score}
import com.bupt.spark.Handler.MsgHandler
import com.bupt.spark.Utils.ConfigManager
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext};

/**
  * Created by guoxingyu on 2018/11/18.
  */
object StreamingDemo {

  val LOG = LoggerFactory.getLogger(StreamingDemo.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: <properties>")
      LOG.error("properties file not exists")
      System.exit(1)
    }

    // init spark
    val configManager = new ConfigManager(args(0))
    val sparkConf = new SparkConf().setAppName(configManager.getProperty("steaming.appName")).setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(configManager.getProperty("streaming.interval").toInt))

    // kafkaConsumerParams
    val kafkaConsumerParams = Map[String, Object](
      "bootstrap.servers" -> configManager.getProperty("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" ->  configManager.getProperty("group.id"),
      "auto.offset.reset" -> configManager.getProperty("auto.offset.reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // kafkaProducerParams
    val props = new Properties()
    props.setProperty("metadata.broker.list",configManager.getProperty("metadata.broker.list"))
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,configManager.getProperty("bootstrap.servers"))
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)

    val inputTopics = Array(configManager.getProperty("input.topics"))
    val outputTopics = configManager.getProperty("output.topics")

    // create stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](inputTopics, kafkaConsumerParams)
    )

    // stream process
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        // clean and pick up msg
        val MsgHandler = new MsgHandler()
        val cleanStreamRDD: RDD[(String, Score)] = rdd.mapPartitions(iter => {
          iter.map(line => {
            if(MsgHandler.cleanAndPickUpMsg(line.value(),configManager)) {
              val scoreInfo = MsgHandler.getScoreBean(line.value()) // json to java bean
              if (scoreInfo != null) {
                (scoreInfo.getId,scoreInfo) // return (id,score bean)
              } else {
                null
              }
            } else {
              null
            }
          })
        }).filter(f => {
          f != null
        })

        // query from hbase, merge json, write into kafka
        cleanStreamRDD.foreachPartition(iter => {
          val lst = iter.toList
          if (!lst.isEmpty) {
            val rowkeys = lst.map(_._1).toSet.toList // get rowkey list

            if (!rowkeys.isEmpty) {
              val res = HBaseUtils.multipleGet(configManager.getProperty("hbase.tableName"),rowkeys).filter(f=> { // get jsonStr from hbase
                !f.isEmpty
              })
              val resMap = res.map(f=> {
                (Bytes.toString(f.getRow),Bytes.toString(f.getValue(Bytes.toBytes(configManager.getProperty("hbase.table.cf"))
                  ,Bytes.toBytes(configManager.getProperty("hbase.table.column")))))
              }).toMap // get result map

              lst.foreach(line => {
                if (resMap.nonEmpty && resMap.get(line._1) != null) {
                  val studentJsonStr = resMap.getOrElse(line._1,null)
                  val studentInfo = MsgHandler.getStudentBean(studentJsonStr)  // get student bean
                  val outputInfo: Output = MsgHandler.getOutputBean(line._2,studentInfo) // merge two bean

                  if (outputInfo != null) {
                    val outputJsonStr: String = JSON.toJSONString(outputInfo, SerializerFeature.WriteNullStringAsEmpty)
                    val producer = new KafkaProducer[String,String](props)
                    println(outputJsonStr)
                    producer.send(new ProducerRecord(outputTopics,"key",outputJsonStr))  // write into kafka
                    producer.close()
                  }
                }
              })
            }
          }
        })
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
