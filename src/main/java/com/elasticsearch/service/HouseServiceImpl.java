package com.elasticsearch.service;

import com.elasticsearch.mapper.HouseRepository;
import com.elasticsearch.model.House;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service(value = "houseService")
public class HouseServiceImpl implements HouseService{

    @Autowired
    private HouseRepository houseRepository;

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    private static final String ES_HOUSE_INDEX_NAME = "searchhousesys";
    private static final String ES_HOUSE_INDEX_TYPE = "house";

    //保存房源信息
    public House add(House house){
        return houseRepository.save(house);
    }

    /**
     * geo_distance: 查找距离某个中心点距离在一定范围内的位置
     * 查找一定范围内内的房源
     *  lat:纬度；  lon: 经度
     */
    public List<House> findHouseWithin(double lat,double lon,double distance){

        Long nowTime = System.currentTimeMillis();
        //查询某经纬度distance米范围内
        GeoDistanceQueryBuilder builder = QueryBuilders.geoDistanceQuery("address").point(lat, lon)
                .distance(distance, DistanceUnit.METERS);

        GeoDistanceSortBuilder sortBuilder = SortBuilders.geoDistanceSort("address")
                .point(lat, lon)
                .unit(DistanceUnit.METERS)
                .order(SortOrder.ASC);

        Pageable pageable = new PageRequest(0, 100);
        NativeSearchQueryBuilder builder1 = new NativeSearchQueryBuilder().withFilter(builder).withSort(sortBuilder).withPageable(pageable);
        SearchQuery searchQuery = builder1.build();
        //queryForList默认是分页，走的是queryForPage，默认10个
        List<House> houseList = elasticsearchTemplate.queryForList(searchQuery, House.class);
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
        return houseList;
    }



    //批量插入数据
    public void bulkIndex(List<House> houseList) {
        int counter = 0;
        try {
            if (!elasticsearchTemplate.indexExists(ES_HOUSE_INDEX_NAME)) {
                elasticsearchTemplate.createIndex(ES_HOUSE_INDEX_TYPE);
            }
            List<IndexQuery> queries = new ArrayList<>();
            for (House house : houseList) {
                IndexQuery indexQuery = new IndexQuery();
                indexQuery.setId(house.getId() + "");
                indexQuery.setObject(house);
                indexQuery.setIndexName(ES_HOUSE_INDEX_NAME);
                indexQuery.setType(ES_HOUSE_INDEX_TYPE);

                //上面的那几步也可以使用IndexQueryBuilder来构建
                //IndexQuery index = new IndexQueryBuilder().withId(person.getId() + "").withObject(person).build();

                queries.add(indexQuery);
                if (counter % 500 == 0) {
                    elasticsearchTemplate.bulkIndex(queries);
                    queries.clear();
                    System.out.println("bulkIndex counter : " + counter);
                }
                counter++;
            }
            if (queries.size() > 0) {
                elasticsearchTemplate.bulkIndex(queries);
            }
            System.out.println("bulkIndex completed.");
        } catch (Exception e) {
            System.out.println("IndexerService.bulkIndex e;" + e.getMessage());
            throw e;
        }
    }



    public void deleteById(String id){
        houseRepository.delete(id);
    }



    public House testSaveHouseIndex(House house){
       House saveHouse = houseRepository.save(house);
       return saveHouse;
   }


   public Iterable<House> searchHouse(String queryString){
       QueryStringQueryBuilder builder = new QueryStringQueryBuilder(queryString);
       Iterable<House> search = houseRepository.search(builder);
       return search;
   }

   public House findHouseById(String id){
       House one = houseRepository.findOne(id);
       return one;
   }

}
