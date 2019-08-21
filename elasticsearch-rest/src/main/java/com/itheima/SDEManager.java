package com.itheima;

import com.google.gson.Gson;
import com.itheima.pojo.Goods;
import com.itheima.repository.GoodsRepository;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Author: wuyue
 * @Date: 2019/8/20 2:43
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SDEManager {

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private GoodsRepository goodsRepository;

    @Test
    public void createIndex(){
        esTemplate.createIndex(Goods.class);
        esTemplate.putMapping(Goods.class);
    }

    @Test
    public void testDoc(){
        //Goods goods = new Goods(1L, "华为 mate20手机", "手机", "华为", 1999.0, "www.shouji.com");
        //goodsRepository.save(goods);

         //goodsRepository.deleteById(1L);


        List<Goods> list = new ArrayList<>();
        list.add(new Goods(1L, "小米手机7", "手机", "小米", 3299.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Goods(2L, "坚果手机R1", "手机", "锤子", 3699.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Goods(3L, "华为META10", "手机", "华为", 4499.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Goods(4L, "小米Mix2S", "手机", "小米", 4299.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Goods(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));

        goodsRepository.saveAll(list);
    }

    @Test
    public void testQuery(){
        /*Iterable<Goods> goodsList = goodsRepository.findAll();
        for (Goods goods : goodsList) {
            System.out.println(goods);
        }*/

        Optional<Goods> optional = goodsRepository.findById(1L);
        System.out.println(optional.get());

        Page<Goods> page = goodsRepository.findAll(PageRequest.of(0, 5));
        List<Goods> content = page.getContent();
        for (Goods goods : content) {
            System.out.println(goods);
        }
    }

    @Test
    public void nativeQuery(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.termQuery("title","小米"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        highlightBuilder.field("title");
        nativeSearchQueryBuilder.withHighlightBuilder(highlightBuilder);
        nativeSearchQueryBuilder.withHighlightFields(new HighlightBuilder.Field("title"));
        AggregatedPage<Goods> aggregatedPage = esTemplate.queryForPage(nativeSearchQueryBuilder.build(), Goods.class,new SearchResultMapperImpl<Goods>());
        List<Goods> goodsList = aggregatedPage.getContent();
        for (Goods goods : goodsList) {
            System.out.println(goods);
        }
    }

    Gson gson = new Gson();

    class SearchResultMapperImpl<T> implements SearchResultMapper{

        @Override
        public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
            long total = searchResponse.getHits().getTotalHits();
            Aggregations aggregations = searchResponse.getAggregations();
            String scrollId = searchResponse.getScrollId();
            float maxScore = searchResponse.getHits().getMaxScore();
            List<T> content = new ArrayList<>();

            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                T t = gson.fromJson(sourceAsString, aClass);

                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("title");
                Text[] fragments = highlightField.getFragments();
                if(fragments!=null&&fragments.length>0){
                    String title_highLight = fragments[0].toString();
                    try {
                        BeanUtils.setProperty(t,"title",title_highLight);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                content.add(t);
            }
            return new AggregatedPageImpl<T>(content,pageable,total,aggregations,scrollId,maxScore);
        }
    }
}
