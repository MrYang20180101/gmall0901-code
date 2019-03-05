package com.atguigu.gmall0901.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0901.dw.common.GmallConstants
import com.atguigu.gmall0901.dw.common.util.MyEsUtil
import com.atguigu.gmall0901.dw.realtime.bean.StartupLog
import com.atguigu.gmall0901.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartupApp {

    def main(args: Array[String]): Unit = {
        
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall0901-realtime-startup")
        val sc: SparkContext = new SparkContext(sparkConf)
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

        val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        val startupDstream: DStream[StartupLog] = recordDstream.map(_.value()).map { jsonString =>
            val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])

            val formatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val logDateTime: String = formatDateTime.format(new Date(startupLog.ts))

            startupLog.logDate = logDateTime.split(" ")(0)
            startupLog.logHourMinute = logDateTime.split(" ")(1)
            startupLog.logHour = logDateTime.split(" ")(1).split(":")(0)
            startupLog

        }

        val filtererStartupLogDstream: DStream[StartupLog] = startupDstream.transform { rdd =>
            println("过滤前共有："+rdd.count())
            val jedis: Jedis = RedisUtil.getJedisClient
            val formatDate = new SimpleDateFormat("yyyy-MM-dd")
            val today: String = formatDate.format(new Date())
            val dauSet: util.Set[String] = jedis.smembers("dau:" + today)
            val dauBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)

            val filteredRDD: RDD[StartupLog] = rdd.filter { startupLog =>
                var isExist = false
                if (dauBC.value != null) {
                    isExist = dauBC.value.contains(startupLog.mid)
                }
                !isExist
            }

            println("过滤后共有："+filteredRDD.count())
            filteredRDD
        }


        //用redis去重
        filtererStartupLogDstream.foreachRDD{ rdd =>

            rdd.foreachPartition{ startupItr =>

                var jedis: Jedis = RedisUtil.getJedisClient

                val list: ListBuffer[Any] = new ListBuffer[Any]()

                for (startupLog <- startupItr) {

                    val daukey: String = "dau:" + startupLog.logDate
                    jedis.sadd(daukey, startupLog.mid)

                    list += startupLog
                }

                jedis.close()
                MyEsUtil.insertBulk(GmallConstants.ES_INDEX_DAU, list.toList)
            }
        }



        ssc.start()
        ssc.awaitTermination()
    }

}
