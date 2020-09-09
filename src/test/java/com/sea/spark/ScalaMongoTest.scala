package com.sea.spark
import java.text.SimpleDateFormat
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.junit.Test

class ScalaMongoTest {

  @Test
  def testOperatorMongo(): Unit ={

    case class MongoConfigs(uri:String, db:String)
    val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.18.129:27017/outputDB"))
    //对数据表建索引
//    mongoClient("swfitDB")("userTable").createIndex(MongoDBObject("mid" -> 1))
    // 如果mongodb中已经有相应的数据库，先删除
//     mongoClient("outputDB")("collectionName").dropCollection()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    mongoClient("outputDB")("collectionName").insert(MongoDBObject("name" -> "Jack%d".format(1), "email" -> "jack%d@sina.com".format(1), "age" ->  25, "birthDay" -> new SimpleDateFormat("yyyy-MM-dd").parse("2016-03-25")))
//    var query02 = MongoDBObject("name" -> "user1")
//    collection.find(query02).forEach(x => println(x))
//    collection.update(query, value,true, true)
//    collection.remove(query)

  }


}
