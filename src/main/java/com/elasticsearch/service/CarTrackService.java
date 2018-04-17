package com.elasticsearch.service;

import com.elasticsearch.model.CarTrack;

import java.util.List;

public interface CarTrackService {

    //存储汽车行驶记录
    void saveCarTrackData(CarTrack carTrack);

    //查询某一时间段内的汽车行驶轨迹数据
    List<CarTrack> getCarTrack(Long startTime, Long endTime);

    //根据id，车牌号参数，全文索引查询数据
    List<CarTrack> getCarTrackByFullText(String queryString);

    //查询全部文档数据
    List<CarTrack> getAllCarTrack();

}
