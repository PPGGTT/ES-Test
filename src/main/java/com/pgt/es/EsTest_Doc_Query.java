package com.pgt.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

public class EsTest_Doc_Query {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //1 全量查询数据  matchAllQuery()
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//                                                    //全量查询的方法
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //2 条件查询  termQuery("age",30)
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//                                                        //条件查询的方法
//        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age",30)));
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //3 分页查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//        //分页查询的方法
//        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        query.from(0);
//        query.size(3);
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //4 查询排序
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//       //指定排序
//        query.sort("age", SortOrder.DESC);
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //5 查询排除
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        //指定排除或者包含的属性
//        String[] includes = {};
//        String[] excludes = {"name"};
//        query.fetchSource(includes,excludes);
//        //添加到request
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        //6 组合查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder();
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        // && 并组合条件查询 and
////        boolQueryBuilder.must(QueryBuilders.matchQuery("age",30));
////        boolQueryBuilder.must(QueryBuilders.matchQuery("name","wanwu"));
////        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name","wanwu"));
//        // || 或组合条件查询 or
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age",30));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("name","wanwu"));
//
//        query.query(boolQueryBuilder);
//        //添加到request
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //7 范围查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder();
//        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
//        //gte()大于等于
//        rangeQuery.gte(20);   //gte和lte去掉e就没有等于含义了
//        //lte()小于等于
//        rangeQuery.lte(40);
//
//        query.query(rangeQuery);
//        //添加到request
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        //8 模糊查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder();                              //指定可以模糊的字符个数
//        FuzzyQueryBuilder fuzziness = QueryBuilders.fuzzyQuery("name", "wannwu").fuzziness(Fuzziness.TWO);
//
//
//        query.query(fuzziness);
//        //添加到request
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }



//        //9 高亮查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder();
//        //查询builder
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "zhangsan");
//        //高亮builder
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.preTags("<font color='red'>");
//        highlightBuilder.postTags("</font>");
//        highlightBuilder.field("name");
//
//        query.highlighter(highlightBuilder);
//        query.query(termQueryBuilder);
//        //添加到request 如果看不到日志的，可以在循环里面打印这个参数：hit.getHighlightFields()
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//            System.out.println(hit.getHighlightFields());
//        }

//        //10 聚合查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");//要查询的Index
//
//        SearchSourceBuilder query = new SearchSourceBuilder();
//        //聚合查询条件
//        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
//        query.aggregation(aggregationBuilder);
//
//
//        request.source(query);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        SearchHits hits = response.getHits();
//
//        System.out.println(hits.getTotalHits());
//        System.out.println(response.getTook());
//
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        //11 分组查询
        SearchRequest request = new SearchRequest();
        request.indices("user");//要查询的Index

        SearchSourceBuilder query = new SearchSourceBuilder();
        //分组查询条件
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGrep").field("age");
        query.aggregation(aggregationBuilder);


        request.source(query);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        for (SearchHit hit: hits) {
            System.out.println(hit.getSourceAsString());
        }
        //关闭ES客户端连接
        esClient.close();
    }
}
