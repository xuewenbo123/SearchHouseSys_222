package com.elasticsearch.controller;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.druid.support.json.JSONWriter;
import com.elasticsearch.model.MapLocation;
import com.elasticsearch.util.ClientUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
@Controller
@RequestMapping("/newTest")
public class NewTestController {

    @Resource
    private ClientUtil clientUtil;


    @RequestMapping("/getDocInDistance/{distance}")
    @ResponseBody
    public String getDocInDistance(@PathVariable("distance") String distance){
        double lat = 39.9047253;
        double lon = 116.395645;
        Double aDouble = Double.valueOf(distance);
        String s = clientUtil.getDocInDistance("girlfriend", "gf_gentle",  lat, lon, aDouble);
        return s;
    }



    @RequestMapping("/saveDocAll")
    @ResponseBody
    public String saveDocAll() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        MapLocation locations = new MapLocation();

        locations.setLat(116.4072154982);
        locations.setLon(39.9047253699);
        Map<String, Object> map = new HashMap<>();
        String id = "1";
        map.put("name","一号");
        map.put("age",18);
        map.put("locations",objectMapper.writeValueAsString(locations));
        map.put("school","中南大学");
        String saveDoc = clientUtil.saveDoc("girlfriend", "gf_gentle", id, map);


        locations.setLat(116.4272154922);
        locations.setLon(39.9047253622);
        Map<String, Object> map2 = new HashMap<>();
        id = "2";
        map2.put("name","二号");
        map2.put("age",16);
        map2.put("locations", objectMapper.writeValueAsString(locations));
        map2.put("school","江南大学");
        clientUtil.saveDoc("girlfriend", "gf_gentle", id, map2);


        locations.setLat(116.42733);
        locations.setLon(39.90433);
        Map<String, Object> map3= new HashMap<>();
        id = "3";
        map3.put("name","三号");
        map3.put("age",21);
        map3.put("locations", objectMapper.writeValueAsString(locations));
        map3.put("school","江南大学研究生");
        clientUtil.saveDoc("girlfriend", "gf_gentle", id, map3);


        locations.setLat(117.42444);
        locations.setLon(40.9044);
        Map<String, Object> map4= new HashMap<>();
        id = "4";
        map4.put("name","四号");
        map4.put("age",21);
        map4.put("locations", objectMapper.writeValueAsString(locations));
        map4.put("school","江南大学研究生");
        clientUtil.saveDoc("girlfriend", "gf_gentle", id, map4);


        locations.setLat(115.55555);
        locations.setLon(40.90555);
        Map<String, Object> map5= new HashMap<>();
        id = "5";
        map5.put("name","五号");
        map5.put("age",21);
        map5.put("locations", objectMapper.writeValueAsString(locations));
        map5.put("school","水木清华");
        clientUtil.saveDoc("girlfriend", "gf_gentle", id, map5);


        return "aaa";
    }




    @RequestMapping("/saveDoc/{id}")
    @ResponseBody
    public String save(@PathVariable("id") String id){
        MapLocation locations = new MapLocation();
        locations.setLat(116.4072154982);
        locations.setLon(39.9047253699);
        Map<String, Object> map = new HashMap<>();
        map.put("name","一号");
        map.put("age",18);
        map.put("locations", locations);
        map.put("school","中南大学");
        String saveDoc = clientUtil.saveDoc("girlfriend", "gf_gentle", id, map);
        return saveDoc;
    }

    @RequestMapping("/addMapping2")
    @ResponseBody
    public String addMapping2() throws  Exception{
        XContentBuilder mapping = getMapping("gf_gentle");
        boolean b = clientUtil.creatIndexAndMapping("girlfriend","gf_gentle",mapping);
        if(b){
            return "addMapping成功！";
        }
        return "addMapping失败！！";
    }


    public XContentBuilder getMapping(String indexType) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indexType)
                .startObject("properties")
                .startObject("@timestamp").field("type", "long").endObject()
                .startObject("name").field("type", "keyword").field("store", true).endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("school").field("type", "text").endObject()
                .startObject("location").field("type", "geo_point").endObject()
                .endObject()
                .endObject()
                .endObject();
        return  mapping;
    }



}
