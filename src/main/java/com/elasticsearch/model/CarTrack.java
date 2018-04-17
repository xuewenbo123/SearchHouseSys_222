package com.elasticsearch.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.GeoPointField;

import java.io.Serializable;
import java.util.Date;
@Document(indexName = "searchhousesys",type = "car_track",indexStoreType="fs",shards=5,replicas=1,refreshInterval="-1")
public class CarTrack implements Serializable {

    @Id
    private String id;
    //汽车牌号
    private String plateNo;
    //定位时间
    private Long SignalTransTime;
    //经纬度
    @GeoPointField
    private String location;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPlateNo() {
        return plateNo;
    }

    public void setPlateNo(String plateNo) {
        this.plateNo = plateNo;
    }

    public Long getSignalTransTime() {
        return SignalTransTime;
    }

    public void setSignalTransTime(Long signalTransTime) {
        SignalTransTime = signalTransTime;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "CarTrack{" +
                "id='" + id + '\'' +
                ", plateNo='" + plateNo + '\'' +
                ", SignalTransTime=" + SignalTransTime +
                ", location='" + location + '\'' +
                '}';
    }
}
