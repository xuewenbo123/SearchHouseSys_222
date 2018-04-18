package com.elasticsearch.controller;

import com.elasticsearch.model.CarTrack;
import com.elasticsearch.service.CarTrackService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping(value = "/carTrack")   //汽车行驶轨迹
public class CarTrackController {

    @Autowired
    private CarTrackService carTrackService;

    @RequestMapping("/saveCarTrack")
    @ResponseBody
    public String insert(){
        CarTrack carTrack = new CarTrack();
        carTrack.setId("4");
        carTrack.setPlateNo("豫B1111");
        carTrack.setSignalTransTime(System.currentTimeMillis());
        carTrack.setLocation("39.955551,117.33333");
        carTrackService.saveCarTrackData(carTrack);
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
