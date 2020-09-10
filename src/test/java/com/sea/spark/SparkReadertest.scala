package com.sea.spark

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test







case class Booking(businessType:String,bookingNo:String,status:String,mawbNo:String,noOfBags:String,milestoneStatus:String);
case class Milestone(number:String,numberType:String,status:String,customer:String,bookingNo:String,lazadaStatusCode:String,actionPlace:String);
case class Order(customer:String,bookingNo:String,bookingDateGMT:String,ecOrderNo:String,courierBillNo:String,bagId:String);
class SparkReadertest {

    // 定义表名
    val MONGODB_BOOKING_COLLECTION = "booking"
    val MONGODB_MILESTONE_COLLECTION = "milestone"
    val MONGODB_ORDER_COLLECTION = "order"

    @Test
    def  test1:Unit=
    {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.swift.uri" -> "mongodb://192.168.18.129:27017/swift",
            "mongo.db" -> "recommender"
        )
        // 创建一个sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("sparkboot")
        // 创建一个SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
//        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        val start = System.currentTimeMillis()
        // 从mongodb加载booking数据
        val bookingDF = spark.read
          .option("uri", config("mongo.swift.uri"))
          .option("collection", MONGODB_BOOKING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .filter($"businessType".equalTo("B2B"))
          .select("businessType","bookingNo","status","noOfBags","milestoneStatus","mawbNo")
          .as[Booking]
          .toDF()
        val totalcost = System.currentTimeMillis()-start
        println("load booking :>>>"+totalcost)
          bookingDF.show(10)

        // 从mongodb加载milestone数据
        val milestoneDF = spark.read
          .option("uri", config("mongo.swift.uri"))
          .option("collection", MONGODB_MILESTONE_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .select("number","bookingNo","numberType","status","customer","lazadaStatusCode","actionPlace")
          .as[Milestone]
          .toDF()
        val totalcost2 = System.currentTimeMillis()-start-totalcost
        println("load milestone :>>>"+totalcost2)
//        milestoneDF.show(10)
//        bookingDF.createOrReplaceTempView("bookings")
//        spark.sql("select bookingNo from bookings").show(10)


        val orderDF = spark.read
          //          .option("uri", mongoConfig.uri)
          .option("uri", config("mongo.swift.uri"))
          .option("collection", MONGODB_ORDER_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
        .select("customer","bookingNo","bookingDateGMT","ecOrderNo","courierBillNo","bagId")
          .as[Order]
          .toDF()
        val totalcost3 = System.currentTimeMillis()-start-totalcost-totalcost2
        println("load order :>>>"+totalcost3)
      // [bookingNo: string], value: [customer: string, bookingNo: string ... 4 more fields],
      val bookingsList = new util.ArrayList[Booking]()
//      orderDF.groupBy("bookingNo").count().toDF().map(x=>{
//        Booking("",x.getAs("bookingNo"),x.getAs("count"),"","","")
//       }).show(100)




      bookingDF.join(milestoneDF,"bookingNo").toDF()
      .write
      .option("uri", config("mongo.swift.uri"))
      .option("collection", "hahha")
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


        spark.stop()

    }

}
