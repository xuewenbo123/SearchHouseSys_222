package com.elasticsearch.util;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Service
public class ESClientUtil {

    @Autowired
    private TransportClient client;

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
    public String deleteById(String index, String type, String id) {
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


    //全文搜索查询，非精确查询；  name:  字段；   text: 对应值
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
     * 获取对应的存储内容
     * @param index
     * @param type
     * @param id
     * @return
     */
    public String getDocAll(String index, String type) {
        SearchResponse response = client.prepareSearch().get();
        SearchHit[] hits = response.getHits().getHits();
        String docStr = "";
        for(SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }

    /**
     *  多条件查询聚合
     * @param index
     * @param type
     * @param id
     * @return
     */
    public  String  multiSearch(String queryString,String name,String text){
        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery(queryString)).setSize(1);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery(name, text)).setSize(1);
        MultiSearchResponse sr = client.prepareMultiSearch().add(srb1).add(srb2).get();
        String docStr = "";
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            SearchHit[] hits = response.getHits().getHits();
            for(SearchHit doc:hits){
                docStr += doc.getSourceAsString();
            }
        }
        return docStr;
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
    public boolean creatIndexAndMapping(String indexName,String type,String mapping){
        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate("dog").addMapping(mapping).get();
        return indexResponse.isAcknowledged();
    }


    //mapping范例
    public void getMapping(){
        String mapping = "{\n" +
                "    \"doginfo\": {\n" +
                "      \"properties\": {\n" +
                "        \"name\": {\n" +
                "          \"type\": \"string\"\n" +
//                "             \"indexAnalyzer\": \"ik\"\n"+
                "        }\n" +

                "        \"age\": {\n" +
                "          \"type\": \"string\"\n" +
//                "             \"indexAnalyzer\": \"ik\"\n"+
                "        }\n" +

                "      }\n" +
                "    }\n" +
                "  }";
    }


    /**
     * 为index，添加mapping
     * @param indexName
     * @param indexType
     * @param mapping
     * @return
     */
    public  boolean createMapping(String indexName,String indexType,String mapping) throws IOException {
        PutMappingRequest putMapping = Requests.putMappingRequest(indexName).type(indexType).source(mapping);
        PutMappingResponse pr = client.admin().indices().putMapping(putMapping).actionGet();
        return pr.isAcknowledged();
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
     * 获取中心点周围指定范围内的文档
     * @param index 索引名称
     * @param locationProperty 地理位置属性名
     * @param lat 纬度
     * @param lon 经度
     * @param distance 距离/Km
     * @return
     */
    public String getDocInDistance(String index,String type,String locationProperty,double lat,double lon,double distance){

        SearchRequestBuilder sr = client.prepareSearch(index).setTypes(type);
//        sr.setFrom(0).setSize(1000);//设置获取的数量为0--1000
        GeoDistanceQueryBuilder geoBuilder = geoDistanceQuery(locationProperty).point(lat, lon).distance(distance, DistanceUnit.METERS);
        sr.setPostFilter(geoBuilder);
        SearchResponse searchResponse = sr.execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for (SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }


    //中心点周边文档查询
    public String getDocMile(String index,String type,String locationProperty,double lat,double lon,double distance){
        QueryBuilder qb = geoDistanceQuery(locationProperty)
                .point(lat, lon)
                .distance(distance, DistanceUnit.KILOMETERS)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(qb).get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for (SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }

    //不分词查询:  字段设置不分词！
    public String getDocByTermQuery(String index, String type,String filed,String value){
        QueryBuilder qb = termQuery(filed, value);
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(qb).execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        String docStr = "";
        for (SearchHit doc:hits){
            docStr += doc.getSourceAsString();
        }
        return docStr;
    }


    //聚合计算，一定范围内每个文档值与原点的距离
    public String getDocByAggs(String index,String type){



        return "";
    }





}
