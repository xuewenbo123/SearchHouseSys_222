package com.elasticsearch.controller;

import com.elasticsearch.util.ClientUtil;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Controller
public class TestController {

    @Autowired
    private ClientUtil clientUtil;




    @RequestMapping("/saveDoc/{id}")
    @ResponseBody
    public String saveDoc(@PathVariable("id") String id){
        Map<String, Object> map = new HashMap<>();
        map.put("age",18);
        map.put("location","39.955566,117.33311");
        map.put("name","张三");
        map.put("school","北京大学");

        return "";
    }




    @RequestMapping("/addMapping")
    @ResponseBody
    public String addMapping() throws  Exception{

        XContentBuilder mapping = getMapping();
        boolean b = clientUtil.creatIndexAndMapping("house",mapping);
        if(b){
            return "addMapping成功！";
        }
        return "addMapping失败！！";
    }



    @RequestMapping("/createIndex/{indexName}")
    @ResponseBody
    public String createIndex(@PathVariable("indexName") String indexName){
        boolean b = clientUtil.creatIndex(indexName);
        if(b){
            return "createIndex成功！";
        }
        return "createIndex失败！！";
    }


    public XContentBuilder getMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("house")
                .startObject("properties")
                .startObject("@timestamp").field("type", "long").endObject()
                .startObject("name").field("type", "keyword").field("store", true).endObject()
                .startObject("school").field("type", "text").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("location").field("type", "geo_point").endObject()
                .endObject()
                .endObject()
                .endObject();
        return  mapping;
    }



}
