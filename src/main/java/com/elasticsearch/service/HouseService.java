package com.elasticsearch.service;

import com.elasticsearch.model.House;

import java.util.Iterator;
import java.util.List;

public interface HouseService {

    House testSaveHouseIndex(House house);

    Iterable<House> searchHouse(String queryString);

    House findHouseById(String id);

    //根据经纬度，查询一定距离内的房源
    List<House> findHouseWithin(double lat, double lon, double distance);

    void deleteById(String id);

    //批量插入数据，测试用
    void bulkIndex(List<House> houseList);

}
