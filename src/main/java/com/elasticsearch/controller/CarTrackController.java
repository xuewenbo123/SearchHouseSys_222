package com.elasticsearch.controller;

import com.elasticsearch.model.CarTrack;
import com.elasticsearch.service.CarTrackService;
import com.elasticsearch.util.ESClientUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


@Controller
@RequestMapping(value = "/carTrack")   //汽车行驶轨迹
public class CarTrackController {

    @Autowired
    private CarTrackService carTrackService;
    @Autowired
    private ESClientUtil esClientUtil;


    @RequestMapping("/saveDoc/{id}")
    @ResponseBody
    public String saveDoc(@PathVariable("id") String id){
        HashMap<String, Object> m = new HashMap<>();
        m.put("plateNo","豫A5596");
        m.put("SignalTransTime",System.currentTimeMillis());
        m.put("location","45.354444,118.3388888");
        String index = esClientUtil.saveDoc("car", "car_track", id, m);
        return "创建成功："+index;
    }

    @RequestMapping("/updateDoc/{id}")
    @ResponseBody
    public String updateDoc(@PathVariable("id") String id){
        HashMap<String, Object> m = new HashMap<>();
        m.put("plateNo","京A55555");
        m.put("SignalTransTime",System.currentTimeMillis());
        m.put("location","39.955566,117.33311");
        String docId = esClientUtil.updateDoc("car","car_track",id,m);
        return "更新成功："+docId;
    }


    @RequestMapping("/deleteDoc/{id}")
    @ResponseBody
    public String deleteDoc(@PathVariable("id") String id){
        String docId = esClientUtil.deleteById("car","car_track",id);
        return "删除成功："+docId;
    }


    @RequestMapping("/getDocById/{id}")
    @ResponseBody
    public String getDocById(@PathVariable("id") String id){
        String doc = esClientUtil.getDocById("car","car_track",id);
        return "获取成功："+doc;
    }

    //创建索引，并添加映射mapping
    @RequestMapping("/creatIndexAndMapping")
    @ResponseBody
    public String creatIndexAndMapping(){
        String mapping = "{\n" +
                "    \"doginfo\": {\n" +
                "      \"properties\": {\n" +

                "        \"name\": {\n" +
                "          \"type\": \"string\"\n" +
                "          \"index\": \"not_analyzed\"\n" +
                "        }\n" +

                "        \"location\": {\n" +
                "          \"type\": \"geo_point\"\n" +
                "        }\n" +

//                "        \"signalTime\": {\n" +
//                "          \"type\": \"long\"\n" +
//                "        }\n" +

                "      }\n" +
                "    }\n" +
                "  }";
        boolean b = esClientUtil.creatIndexAndMapping("dog", "doginfo", mapping);
        if(b==true){
            return "映射添加成功！";
        }
        return "失败";
    }


    @RequestMapping("/getDocAll")
    @ResponseBody
    public String getDocAll(){
        String docAll = esClientUtil.getDocAll("car", "car_track");
        return docAll;
    }

    //字段值匹配查询
    @RequestMapping("/multiSearch")
    @ResponseBody
    public String multiSearch(){
        String multiSearch = esClientUtil.multiSearch("京A2222", "id", "2");
        return multiSearch;
    }


    //字段值匹配查询：
    @RequestMapping("/fullTextQuery")
    @ResponseBody
    public String fullTextQuery(){
        String result = esClientUtil.fullTextQuery("car","car_track","plateNo","京A2222");
        return result;
    }

    //字段值匹配查询：
    @RequestMapping("/getDocByTermQuery/{queryString}")
    @ResponseBody
    public String getDocByTermQuery(@PathVariable("queryString") String queryString){
        String result = esClientUtil.getDocByTermQuery("car","car_track","plateNo",queryString);
        return result;
    }




    //获取指定中心点一定范围内的文档
    @RequestMapping("/getDocInDistance/{distance}")
    @ResponseBody
    public String getDocInDistance(@PathVariable("distance") String distance){
        double lat = 39.929986;
        double lon = 116.395645;
        String docInDistance = esClientUtil.getDocMile("car", "car_track","location", lat, lon, Double.valueOf(distance));
        return docInDistance;
    }














    @RequestMapping("/saveCarTrack")
    @ResponseBody
    public String insert(){
        CarTrack carTrack = new CarTrack();
        carTrack.setId("4");
        carTrack.setPlateNo("豫B1111");
        carTrack.setSignalTransTime(System.currentTimeMillis());
        carTrack.setLocation("39.955551,117.33333");
        return "存储成功！";
    }


    @RequestMapping("/selectAll")
    @ResponseBody
    public List<CarTrack> getCarTrack(){
        List<CarTrack> carTrack = carTrackService.getAllCarTrack();
        return carTrack;
    }


    @RequestMapping("/selectCarTrackBy/{queryString}")
    @ResponseBody
    public List<CarTrack> getCarTrackByFullText(@PathVariable("queryString") String queryString){
        List<CarTrack> carTrack = carTrackService.getCarTrackByFullText(queryString);
        return carTrack;
    }


    @RequestMapping("/selectCarTrackWithin/{distance}")
    @ResponseBody
    public List<CarTrack> getCarTrackWithin(@PathVariable("distance") double distance){
        double lat = 39.929986;
        double lon = 116.395645;
//        double distance = 100; //100米
        List<CarTrack> carTrack = carTrackService.getCarWithin(lat, lon, distance);
        return carTrack;
    }


    /*
     * 说明：查询某时间段内的某id汽车行驶轨迹
     */
    @RequestMapping("/selectByTime/{id}")
    @ResponseBody
    public List<CarTrack> getCarTrackByTime(@PathVariable("id") String id){
        Long startTime = 1523943194836L;
        Long endTime = 1523943951673L;
        List<CarTrack> carTrack = carTrackService.getCarTrack(id, startTime, endTime);
        return carTrack;
    }

    @RequestMapping("/getById/{id}")
    @ResponseBody
    public String getById(@PathVariable("id") String id){
        String byId = carTrackService.getById("searchhousesys", "car_track", id);
        return byId;
    }


}
