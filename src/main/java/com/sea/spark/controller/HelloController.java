package com.sea.spark.controller;

import com.sea.spark.scala.service.HelloService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @PACKAGE : com.sea.spark.controller
 * @Author :  Sea
 * @Date : 9/9/20 6:25 PM
 * @Desc :
 **/
@RestController
public class HelloController {
    @Autowired
    HelloService  helloService;

    @GetMapping("hello/{world}")
   public  String  helloword(@PathVariable("world") String world){
      return helloService.sayHello(world);
   }

}
