package com.elasticsearch.controller;

import com.elasticsearch.model.MapLocation;
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
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/test")
public class TestController {

    @Autowired
    private ClientUtil clientUtil;


    @RequestMapping("/getDocByTermAndSort/{name}/{startTime}/{endTime}")
    @ResponseBody
    public List<Map<String, Object>> getDocByTermAndSort(@PathVariable("name") String name,@PathVariable("startTime") String startTime,@PathVariable("endTime") String endTime){
        List<Map<String, Object>> mapList = clientUtil.fffff("student", "stu_type",name,startTime,endTime);
        return mapList;
    }


    @RequestMapping("/saveDocSpecial")
    @ResponseBody
    public String saveDocSpecial(){
        String id = "";
        Map<String, Object> map = new HashMap<>();

        map.put("age",21);
        map.put("name","李白");
        map.put("school","复旦大学");
        map.put("location","39.96666,117.22666");
        map.put("createTime",System.currentTimeMillis()+5000);
        id = "6";
        String saveDoc = clientUtil.saveDoc("student", "stu_type", id, map);

        map.put("age",21);
        map.put("name","李白");
        map.put("school","复旦大学");
        map.put("location","39.77777,117.77777");
        map.put("createTime",System.currentTimeMillis()+3000);
        id = "7";
        clientUtil.saveDoc("student", "stu_type", id, map);

        map.put("age",21);
        map.put("name","李白");
        map.put("school","复旦大学");
        map.put("location","40.98888,117.38888");
        map.put("createTime",System.currentTimeMillis());
        id = "8";
        clientUtil.saveDoc("student", "stu_type", id, map);


        map.put("age",21);
        map.put("name","李白");
        map.put("school","复旦大学");
        map.put("location","39.99999,117.39999");
        map.put("createTime",System.currentTimeMillis());
        id = "9";
        clientUtil.saveDoc("student", "stu_type", id, map);

        return saveDoc;
    }


    @RequestMapping("/getDocByTimeRange/{startTime}/{endTime}")
    @ResponseBody
    public String getDocByTimeRange(@PathVariable("startTime") String startTime,@PathVariable("endTime") String endTime){
        String s = clientUtil.getDocByTimeRange("student", "stu_type",startTime,endTime);
        return s;
    }


    @RequestMapping("/getDocAsMap")
    @ResponseBody
    public  List<Map<String, Object>> getDocAsMap(){
        List<Map<String, Object>> mapList = clientUtil.getDocAsMap("student", "stu_type");
        return mapList;
    }


    @RequestMapping("/getDocAndSort")
    @ResponseBody
    public String getDocAndSort(){
        String s = clientUtil.getDocAndSort("student", "stu_type");
        return s;
    }


    @RequestMapping("/geoFoundDoc/{distance}")
    @ResponseBody
    public String geoFoundDoc(@PathVariable("distance") String distance){
        double lat = 39.929986;
        double lon = 116.395645;
        Double aDouble = Double.valueOf(distance);
        String s = clientUtil.getDocInDistance("student", "stu_type",  lat, lon, aDouble);
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
        String s = clientUtil.geoDocCount("student", "stu_type",lat,lon);
        return s;
    }



    @RequestMapping("/saveDocAll")
    @ResponseBody
    public String saveDocAll(){

        final String index = "student";
        final String type = "stu_type";

        Map<String, Object> map = new HashMap<>();
        map.put("age",80);
        map.put("location","39.91111,117.22111");
        map.put("name","张三丰");
        map.put("school","中南大学");
        map.put("createTime",System.currentTimeMillis()+5000);
        String id = "1";
        String saveDoc = clientUtil.saveDoc(index, type, id, map);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("age",21);
        map2.put("location","39.911122,117.33322");
        map2.put("name","李白");
        map2.put("school","复旦大学");
        map2.put("createTime",System.currentTimeMillis()+3000);
        id = "2";
        clientUtil.saveDoc(index, type, id, map2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("age",21);
        map3.put("location","40.911333,116.33333");
        map3.put("name","张三");
        map3.put("school","复旦大学研究生");
        map3.put("createTime",System.currentTimeMillis());
        id = "3";
        clientUtil.saveDoc(index, type, id, map3);

        Map<String, Object> map4 = new HashMap<>();
        map4.put("age",32);
        map4.put("location","40.911344,118.33344");
        map4.put("name","孟浩然");
        map4.put("school","清华大学");
        map4.put("createTime",System.currentTimeMillis());
        id = "4";
        clientUtil.saveDoc(index, type, id, map4);

        Map<String, Object> map5 = new HashMap<>();
        map5.put("age",32);
        map5.put("location","41.911555,117.33555");
        map5.put("name","白居易");
        map5.put("school","北京大学");
        map5.put("createTime",System.currentTimeMillis());
        id = "5";
        clientUtil.saveDoc(index, type, id, map5);

        return saveDoc;
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
        map.put("createTime",System.currentTimeMillis());
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
//                .startObject("@timestamp").field("type", "long").endObject()
                .startObject("name").field("type", "keyword").field("store", true).endObject()
                .startObject("school").field("type", "text").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("createTime").field("type", "date").field("format", "epoch_millis").endObject()
//                .startObject("createTime2").field("type", "date").field("format", "strict_date_optional_time").endObject()
//                .startObject("createTime3").field("type", "date").field("format", "strict_date_optional_time||epoch_millis").endObject()
//                .startObject("createTime4").field("type", "date").field("format", "dd/MMM/yyyy:HH:mm:ss Z").endObject()

                .startObject("location").field("type", "geo_point").endObject()
                .endObject()
                .endObject()
                .endObject();
        return  mapping;
    }

}
