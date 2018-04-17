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

}
