package com.elasticsearch.controller;

import com.elasticsearch.model.House;
import com.elasticsearch.service.HouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

@Controller
@RequestMapping(value = "house")
public class HouseController {

    @Autowired
    private HouseService houseService;

    @RequestMapping("/add")
    @ResponseBody
    public House testSaveHouseIndex(House house){
//        house.setId(1L);
//        house.setAddress("40.715,-74.011");
        House saveHouse = houseService.testSaveHouseIndex(house);
        System.out.println("存储成功！");
        return saveHouse;
    }


    @RequestMapping("/queryById/{id}")
    @ResponseBody
    public House searchHouseById(@PathVariable("id") String id) {
        House houseById = houseService.findHouseById(id);
        return houseById;
    }

    @RequestMapping("/query/{queryString}")
    @ResponseBody
    public List<House> searchAll(@PathVariable("queryString") String queryString){
        Iterable<House> houseIterable = houseService.searchHouse(queryString);
        Iterator<House> iterator = houseIterable.iterator();
        List<House> hList = new ArrayList<>();
        while(iterator.hasNext()){
            House house = iterator.next();
            hList.add(house);
        }
        return hList;
    }


    /**
     * geo_distance: 查找距离某个中心点距离在一定范围内的位置
     * geo_bounding_box: 查找某个长方形区域内的位置
     * geo_distance_range: 查找距离某个中心的距离在min和max之间的位置
     * geo_polygon: 查找位于多边形内的地点。
     * sort可以用来排序
     */
    @GetMapping("/queryHouse")
    @ResponseBody
    public Object query() {
        double lat = 39.929986;
        double lon = 116.395645;
        double distance = 200; //2000米

        List<House> hList = houseService.findHouseWithin(lat, lon, distance);
        return hList;
    }

    @RequestMapping("/delete/{id}")
    @ResponseBody
    public String deleteById(@PathVariable("id") String id){
        houseService.deleteById(id);
        return "删除成功！";
    }


    //测试插入数据
    @RequestMapping(value = "insert")
    @ResponseBody
    public String insertData(){
        double lat = 39.929986;
        double lon = 116.395645;
        List<House> houseList = new ArrayList<>(10000);
        for (int i = 100000; i < 110000; i++) {
            double max = 0.00001;
            double min = 0.000001;
            Random random = new Random();
            double s = random.nextDouble() % (max - min + 1) + max;
            DecimalFormat df = new DecimalFormat("######0.000000");
            // System.out.println(s);
            String lons = df.format(s + lon);
            String lats = df.format(s + lat);
            Double dlon = Double.valueOf(lons);
            Double dlat = Double.valueOf(lats);
            House house = new House();
            house.setId(Long.valueOf(i));
            house.setAddress(dlat + "," + dlon);
            houseList.add(house);
        }
        houseService.bulkIndex(houseList);
        return "数据插入成功！";
    }



}
