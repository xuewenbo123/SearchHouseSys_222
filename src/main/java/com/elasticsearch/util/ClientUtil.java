package com.elasticsearch.util;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

@Service
public class ClientUtil {

    @Autowired
    private TransportClient client;

    /**
     * 创建一个空索引
     * @param indexName
     * @return
     */
    public boolean creatIndex(String indexName){
        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(indexName).execute().actionGet();
        return indexResponse.isAcknowledged();
    }


    /**
     * 创建索引并添加映射
     * @param indexName
     * @param type
     * @param mapping
     * @return     index 若已经存在则不能添加mapping
     */
    public boolean creatIndexAndMapping(String indexName,String type,XContentBuilder mapping){
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 5);//分片数量
        settings.put("number_of_replicas", 0);//复制数量
        settings.put("refresh_interval", "10s");//刷新时间
        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(indexName);
        cib.setSettings(settings);
        cib.addMapping(type, mapping);
        CreateIndexResponse indexResponse = cib.execute().actionGet();
        return indexResponse.isAcknowledged();
    }


    /**
     * 创建type（存在则进行更新）
     * @param index 索引名称
     * @param type type名称
     * @param o 要设置type的object
     * @return
     */
    public boolean createType(String index, String type, Map<String,Object> o) {
        if (!isIndexExist(index)) {
            System.out.println("索引不存在");
            return false;
        }
        try {
            // 若type存在则可通过该方法更新type
            return client.admin().indices().preparePutMapping(index).setType(type).setSource(o).get().isAcknowledged();
        } catch (Exception e) {
            System.out.println("创建type失败");
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    public boolean isIndexExist(String index) {
        return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    }


    /**
     * 判断某个索引下type是否存在
     * @param index
     * @param type
     * @return
     */
    public boolean isTypeExist(String index, String type) {
        return client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
    }


    /**
     * 添加纪录: 索引不存在时候，直接创建
     * @param index 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @param json 是key:value数据类型，可以代表json结构.
     * @return
     */
    public String saveDoc(String index, String type,String id, Map<String,Object> json){
        IndexResponse response = client.prepareIndex(index, type,id).setSource(json).get();
        return response.getIndex();
    }


    /**
     * 更新文档
     * @param index
     * @param type
     * @param id
     * @param doc
     * @return
     */
    public String updateDoc(String index, String type, String id, Map<String,Object> doc) {
        UpdateResponse updateResponse = client.prepareUpdate(index, type, id).setDoc(doc).get();
        return updateResponse.getId();
    }


    /**
     * 删除指定id文档
     * @param index
     * @param type
     * @param id
     * @return
     */
    public String deleteDoc(String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id).get();
        return response.getId();
    }


    /**
     * 根据id获取对应的存储内容
     * @param index
     * @param type
     * @param id
     * @return
     */
    public String getDocById(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        if (response.isExists()) {
            return response.getSourceAsString();
        }
        return null;
    }


    /**
     * 说明：字段type区分是否精准查询
     * @param index
     * @param type
     * @param name  : 查询字段filed
     * @param text  : 字段对应value
     * @return
     */
    public String fullTextQuery(String index, String type, String name,String text) {
        QueryBuilder qb = matchQuery(name,text);
        SearchResponse searchResponse = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
                // 2.SearchType.SCAN = 扫描查询,无序
                // 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for(SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }

    /**
     * 获取index对应type的所有存储文档
     * @param index
     * @param type
     * @return
     */
    public String getDocAll() {
        SearchResponse response = client.prepareSearch().get();
        SearchHit[] hits = response.getHits().getHits();
        String docStr = "";
        for(SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }



    /**
     * 聚合查询，获取中心点不同距离范围内文档数量
     * @param index
     * @param type
     * @return
     */
    public String geoDocCount(String index,String type,double lat,double lon){
        AggregationBuilder aggregation	= AggregationBuilders
                .geoDistance("agg",	new GeoPoint(lat,lon))
                .field("location")   //字段名
                .unit(DistanceUnit.KILOMETERS)
                .addUnboundedTo(5.0)
                .addRange(5.0,	10.0)
                .addRange(10.0,	500.0)
                .addRange(500.0,1000.0);
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).addAggregation(aggregation).execute().actionGet();
        Range agg = searchResponse.getAggregations().get("agg");
        List<? extends Range.Bucket> buckets = agg.getBuckets();
        String s = "";
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();    // key as String
            Number from = (Number) entry.getFrom(); // bucket from value
            Number to = (Number) entry.getTo();     // bucket to value
            long docCount = entry.getDocCount();    // Doc count
            String fromAsString = entry.getFromAsString();
            String toAsString = entry.getToAsString();
            //此处拼接结果
            s += "查询范围：["+from+","+to+"]; 文档数量："+docCount+" | ";
        }
        return s;
    }

    //获取矩形内的文档
    public String getDocInBox(String index,String type){

        QueryBuilder qb = geoBoundingBoxQuery("location")	//field
         .setCorners(38.73,	-80,
                 //bounding	box	top	left	point
                 50.717,	120);//bounding	box	bottom	right	point
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(qb).execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for(SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }



    /**
     * 获取中心点周围指定范围内的文档
     * @param index 索引名称
     * @param location 地理位置属性名
     * @param lat 纬度
     * @param lon 经度
     * @param distance 距离/Km
     * @return
     */
    public String getDocInDistance(String index,String type,double lat,double lon,double distance){
        QueryBuilder qb = geoDistanceQuery("location")		//field
                .point(lat,	lon)
                .distance(distance,	DistanceUnit.KILOMETERS);
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(qb).execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for (SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }



















    //创建mapping方法实例
    public XContentBuilder getMapping() throws IOException{

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("wsce")
                //注释代码放开出错，新版本es不支持？ 有待查证；
//                .startObject("_ttl")//有了这个设置,就等于在这个给索引的记录增加了失效时间,
//                //ttl的使用地方如在分布式下,web系统用户登录状态的维护.
//                .field("enabled", true)//默认的false的
//                .field("default", "5m")//默认的失效时间,d/h/m/s 即天/小时/分钟/秒
//                .field("store", true)
//                .field("index", "not_analyzed")
//                .endObject()
//                .startObject("_timestamp")//这个字段为时间戳字段.即你添加一条索引记录后,自动给该记录增加个时间字段(记录的创建时间),搜索中可以直接搜索该字段.
//                .field("enabled", true)
//                .field("store", true)
//                .field("index", "not_analyzed")
//                .endObject()
                //properties下定义的name等等就是属于我们需要的自定义字段了,相当于数据库中的表字段 ,此处相当于创建数据库表

              //新版本es，yes,no,0,1全部用true,false替换
               .startObject("properties")
                .startObject("@timestamp").field("type", "long").endObject()
                .startObject("name").field("type", "string").field("store", "yes").endObject()
                .startObject("home").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("now_home").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("height").field("type", "double").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("birthday").field("type", "com/gourd/erwa/date").field("format", "YYYY-MM-dd").endObject()
                .startObject("isRealMen").field("type", "boolean").endObject()
                .startObject("location").field("lat", "double").field("lon", "double").endObject()
                .endObject()
                .endObject()
                .endObject();
        return  mapping;
    }





}
