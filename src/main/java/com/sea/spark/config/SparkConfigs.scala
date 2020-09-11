package com.sea.spark.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class SparkConfigs {

  val  appName :String= "sparkExp"

  val  master:String = "local[4]";

//  @Bean
//  @ConditionalOnMissingBean
//  def  sparkConf():SparkConf = {
//    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster(master)
//    return conf;
//  }


  @Bean
  @ConditionalOnMissingBean
  def  sparkSession():SparkSession ={
    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    // 创建一个SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    return  spark;
  }



//  @Bean
//  @ConditionalOnMissingBean
//  public JavaSparkContext javaSparkContext() throws Exception {
//    return new JavaSparkContext(sparkConf());
//  }


//  @Bean
//  @ConditionalOnMissingBean
//  public HiveContext hiveContext() throws Exception {
//    return new HiveContext(javaSparkContext());
//  }

}
