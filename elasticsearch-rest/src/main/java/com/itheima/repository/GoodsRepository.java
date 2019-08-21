package com.itheima.repository;

import com.itheima.pojo.Goods;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface GoodsRepository extends ElasticsearchRepository<Goods,Long> {

    List<Goods> findByTitle(String title);

    List<Goods> findByPriceBetween(double low,double high);

    List<Goods> findByTitleAndPriceBetween(String title,double low,double high);

    List<Goods> findByTitleOrPriceBetween(String title,double low,double high);
}
