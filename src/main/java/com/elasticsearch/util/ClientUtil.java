package com.elasticsearch.util;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
    public boolean creatIndexAndMapping(String indexName,XContentBuilder mapping){
        String s = mapping.toString();
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 5);//分片数量
        settings.put("number_of_replicas", 0);//复制数量
        settings.put("refresh_interval", "10s");//刷新时间
        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(indexName);
        cib.setSettings(settings);
        cib.addMapping(indexName, mapping);
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






























    //创建mapping方法实例
    public XContentBuilder getMapping() throws IOException{

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("house")
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
