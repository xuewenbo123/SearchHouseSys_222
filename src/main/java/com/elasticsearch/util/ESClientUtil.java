package com.elasticsearch.util;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;

public class ESClientUtil {

    @Autowired
    private TransportClient client;

    /**
     * 创建索引
     * @param index 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @param json 是key:value数据类型，可以代表json结构.
     * @return
     */
    public String createIndex(String index, String type,  Map<String,Object> json){
        IndexResponse response = client.prepareIndex(index, type).setSource(json).get();
        return response.getIndex();
    }


    /**
     * 更新文档
     *
     * @param index
     * @param type
     * @param id
     * @param doc
     * @return
     */
    public String updateDoc(String index, String type, String id, Object doc) {
        UpdateResponse updateResponse = client.prepareUpdate(index, type, id).setDoc(doc).get();
        return updateResponse.getId();
    }

    /**
     * 删除索引
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
    public String getIdx(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        if (response.isExists()) {
            return response.getSourceAsString();
        }
        return null;
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
     * 判断索引是否存在
     * @param index
     * @return
     */
    public boolean isIndexExist(String index) {
        return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    }

    /**
     * 创建type（存在则进行更新）
     * @param index 索引名称
     * @param type type名称
     * @param o 要设置type的object
     * @return
     */
    public boolean createType(String index, String type, Object o) {
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


    public List getObjWithin(String index,String locationProperty,double lat,double lon,double distance){
        GeoDistanceQueryBuilder query = geoDistanceQuery(locationProperty);
        QueryBuilder qb = query.point(lat, lon).distance(distance, DistanceUnit.KILOMETERS);
        SearchResponse response = client.prepareSearch(index).setQuery(qb).get();
        return null;
    }


}
