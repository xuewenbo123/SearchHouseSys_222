package com.elasticsearch.service;

import com.elasticsearch.model.CarTrack;
import com.elasticsearch.util.ESClientUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service(value = "carTrackService")
public class CarTrackServiceImpl implements CarTrackService {

    @Autowired
    private  TransportClient client;

    private static final String CARTRACK_INDEX_NAME = "searchhousesys";
    private static final String CARTRACK_INDEX_TYPE = "car_track";


    /*
    * 说明：存储汽车行驶轨迹数据
    */
    public void saveCarTrackData(CarTrack carTrack){

    }


    /*
    * 说明：根据时间段查询某id汽车行驶数据
    */
    public List<CarTrack> getCarTrack(String id,Long startTime,Long endTime){

        return null;
    }


    //根据参数，全文匹配获取文档
    public List<CarTrack> getCarTrackByFullText(String queryString) {

        return null;
    }


    //查询全部文档数据
    public  List<CarTrack> getAllCarTrack(){

        return null;
    }

    //获取距离中心点一定距离的文档数据: lat纬度，lon经度
    public List<CarTrack> getCarWithin(double lat,double lon,double distance){

        return null;
    }

   public String getById(String index,String type,String id){
       GetResponse response = client.prepareGet(index, type, id).get();
       if (response.isExists()) {
           return response.getSourceAsString();
       }
       return null;
   }

}
