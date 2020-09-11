package com.sea.spark.scala.service



import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

case class Booking(businessType:String,bookingNo:String,status:String,mawbNo:String,noOfBags:String,milestoneStatus:String);
@Service
class HelloService  @Autowired()(
                                private val sparkSession: SparkSession)
{

  def  sayHello(work:String)=
  {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.swift.uri" -> "mongodb://192.168.18.129:27017/swift",
      "mongo.db" -> "recommender"
    )
    import sparkSession.implicits._
    val start = System.currentTimeMillis()
    // 从mongodb加载booking数据
    val bookingDF = sparkSession.read
      .option("uri", config("mongo.swift.uri"))
      .option("collection", "booking")
      .format("com.mongodb.spark.sql")
      .load()
      .select("businessType","bookingNo","status","noOfBags","milestoneStatus","mawbNo")
      .filter($"businessType".equalTo("B2B"))
      .as[Booking]
      .toDF()
    val totalcost = System.currentTimeMillis()-start
    println(totalcost)
    bookingDF.show(10)
    work
  }





}
