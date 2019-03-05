package com.atguigu.gmall0901.dw.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0901.dw.common.GmallConstants;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    //声明仓库采集的logger工具
    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public String log(@RequestParam("log") String logJson){
        System.out.println(logJson);
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());
        sendKafka(jsonObject);
        logger.info(jsonObject.toJSONString());

        return   "success";
    }

    private void sendKafka(JSONObject jsonObject){

        if (jsonObject.getString("type").equals("startup")){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }
    }
}
