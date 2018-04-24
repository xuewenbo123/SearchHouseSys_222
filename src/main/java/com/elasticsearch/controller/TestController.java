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
@RequestMapping("/test")
public class TestController {

    @Autowired
    private ClientUtil clientUtil;


    //方法报错，工具类不能正常执行
    @RequestMapping("/geoFoundDoc/{distance}")
    @ResponseBody
    public String geoFoundDoc(@PathVariable("distance") String distance){
        double lat = 39.929986;
        double lon = 116.395645;
        Double aDouble = Double.valueOf(distance);
        String s = clientUtil.geoFoundDoc("student", "stu_type",  lat, lon, aDouble);
        return s;
    }



    @RequestMapping("/getDocInBox")
    @ResponseBody
    public String getDocInBox(String distance){
        String s = clientUtil.getDocInBox("student", "stu_type");
        return s;
    }


    @RequestMapping("/geoDocCount")
    @ResponseBody
    public String getDocInDistance(String distance){
        double lat = 39.929986;
        double lon = 116.395645;
        Double aDouble = Double.valueOf(distance);
//        String s = clientUtil.getDocInDistance("student", "stu_type", "location", lat, lon, aDouble);
//        String s = clientUtil.geoQuery("student", "stu_type", "location", lat, lon, distance);
        String s = clientUtil.geoDocCount("student", "stu_type",lat,lon);
        return s;
    }


    @RequestMapping("/getDocAll")
    @ResponseBody
    public String getDocAll(){
        String saveDoc = clientUtil.getDocAll();
        return saveDoc;
    }


    @RequestMapping("/fullTextQuery/{name}")
    @ResponseBody
    public String fullTextQuery(@PathVariable("name") String name){
        String saveDoc = clientUtil.fullTextQuery("student", "stu_type", "school",name);
        return saveDoc;
    }


    @RequestMapping("/getDocById/{id}")
    @ResponseBody
    public String getDocById(@PathVariable("id") String id){
        String saveDoc = clientUtil.getDocById("student", "stu_type", id);
        return saveDoc;
    }


    @RequestMapping("/deleteDoc/{id}")
    @ResponseBody
    public String deleteDoc(@PathVariable("id") String id){
        Map<String, Object> map = new HashMap<>();
        map.put("age",21);
        map.put("location","39.911111,117.33333");
        map.put("name","李白");
        map.put("school","复旦大学");
        String saveDoc = clientUtil.deleteDoc("student", "stu_type", id);
        return saveDoc;
    }



    @RequestMapping("/updateDoc/{id}")
    @ResponseBody
    public String updateDoc(@PathVariable("id") String id){
        Map<String, Object> map = new HashMap<>();
        map.put("age",21);
        map.put("location","39.911111,117.33333");
        map.put("name","李白");
        map.put("school","复旦大学");
        String saveDoc = clientUtil.updateDoc("student", "stu_type", id, map);
        return saveDoc;
    }


    @RequestMapping("/saveDoc/{id}")
    @ResponseBody
    public String saveDoc(@PathVariable("id") String id){
        Map<String, Object> map = new HashMap<>();
        map.put("age",80);
        map.put("location","39.92211,117.22111");
        map.put("name","张三丰");
        map.put("school","中南大学");
        String saveDoc = clientUtil.saveDoc("student", "stu_type", id, map);
        return saveDoc;
    }



    @RequestMapping("/addMapping")
    @ResponseBody
    public String addMapping() throws  Exception{
        XContentBuilder mapping = getMapping("stu_type");
        boolean b = clientUtil.creatIndexAndMapping("student","stu_type",mapping);
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


    public XContentBuilder getMapping(String indexType) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indexType)
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
