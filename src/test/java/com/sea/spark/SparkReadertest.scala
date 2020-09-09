package com.sea.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test


//"number": "UFL-BK2020072315563672466",
//"numberType": "bookingNo",
//"status": "DAM",
//"customer": "HKG041049",
//"numberApp": "swift",
//"bookingNo": "UFL-BK2020072315563672466",
//"lazadaStatusCode": "remark_parcel_damage_by_lh",
//"actionPlace": "HKG",
//"actionTimeLoc": "2020-07-20 14:20:52",
//"actionTimeTz": "GMT+8",
//"actionTimeGmt": "2020-07-20 14:20:52",
//"actionBy": "Sea",


case class Booking(businessType:String,bookingNo:String,status:String,mawbNo:String,noOfBags:String,milestoneStatus:String);
case class Milestone(number:String,numberType:String,status:String,customer:String,bookingNo:String,lazadaStatusCode:String,actionPlace:String);
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
            "mongo.uri" -> "mongodb://192.168.18.129:27017/swift",
            "mongo.db" -> "recommender"
        )
        // 创建一个sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("sparkboot")
        // 创建一个SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        // 从mongodb加载booking数据
        val bookingDF = spark.read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_BOOKING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .filter($"businessType".equalTo("B2B"))
          .select("businessType","bookingNo","status","noOfBags","milestoneStatus","mawbNo")
          .as[Booking]
          .toDF()
          bookingDF.show(10)

        // 从mongodb加载milestone数据
        val ratingDF = spark.read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_MILESTONE_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .select("number","bookingNo","numberType","status","customer","lazadaStatusCode","actionPlace")
          .as[Milestone]
          .toDF()
        ratingDF.show(10)





    }

}
