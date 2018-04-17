package com.elasticsearch.service;

import com.elasticsearch.mapper.CarTrackRepository;
import com.elasticsearch.model.CarTrack;
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
    private CarTrackRepository carTrackRepository;

    private TransportClient client;

    private static final String CARTRACK_INDEX_NAME = "searchhousesys";
    private static final String CARTRACK_INDEX_TYPE = "car_track";


    /*
    * 说明：存储汽车行驶轨迹数据
    */
    public void saveCarTrackData(CarTrack carTrack){
        carTrackRepository.save(carTrack);
    }


    /*
    * 说明：根据时间段查询某id汽车行驶数据
    */
    public List<CarTrack> getCarTrack(String id,Long startTime,Long endTime){
        //需要完善Elasticsearch的and查询
        RangeQueryBuilder rb = rangeQuery("SignalTransTime") .from(startTime).to(endTime).includeLower(true).includeUpper(false);
        Iterable<CarTrack> search = carTrackRepository.search(rb);
        Iterator<CarTrack> iterator = search.iterator();
        List<CarTrack> carTracks = new ArrayList<>();
        while (iterator.hasNext()){
            carTracks.add(iterator.next());
        }
        return carTracks;
    }


    //根据参数，全文匹配获取文档
    public List<CarTrack> getCarTrackByFullText(String queryString) {
        QueryBuilder qb = multiMatchQuery(queryString, "id", "plateNo"  );
        Iterable<CarTrack> search = carTrackRepository.search(qb);
        Iterator<CarTrack> iterator = search.iterator();
        List<CarTrack> carTracks = new ArrayList<>();
        while (iterator.hasNext()){
            carTracks.add(iterator.next());
        }
        return carTracks;
    }


    //查询全部文档数据
    public  List<CarTrack> getAllCarTrack(){
        MatchAllQueryBuilder mb = matchAllQuery();
        Iterable<CarTrack> search = carTrackRepository.search(mb);
        Iterator<CarTrack> iterator = search.iterator();
        ArrayList<CarTrack> carTracks = new ArrayList<>();
        while (iterator.hasNext()){
            carTracks.add(iterator.next());
        }
        return carTracks;
    }

    //获取距离中心点一定距离的文档数据: lat纬度，lon经度
    public List<CarTrack> getCarWithin(double lat,double lon,double distance){
        QueryBuilder qb = geoDistanceQuery("location")  //field
                .point(lat, lon)                                 //center point
                .distance(distance, DistanceUnit.KILOMETERS);
        Iterable<CarTrack> search = carTrackRepository.search(qb);
        Iterator<CarTrack> iterator = search.iterator();
        List<CarTrack> carTracks = new ArrayList<>();
        while (iterator.hasNext()){
            carTracks.add(iterator.next());
        }
        return carTracks;
    }




}
