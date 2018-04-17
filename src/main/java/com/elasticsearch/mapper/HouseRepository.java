package com.elasticsearch.mapper;

import com.elasticsearch.model.House;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HouseRepository extends ElasticsearchRepository<House,String> {

}
