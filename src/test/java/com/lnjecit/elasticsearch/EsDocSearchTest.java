package com.lnjecit.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lnjecit.elasticsearch.domain.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EsDocSearchTest {

    private RestHighLevelClient client;
    public ObjectMapper objectMapper;
    private final static String USER_INDEX = "user_test";

    @Before
    public void setUp() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        System.out.println("connect es success");

        objectMapper = new ObjectMapper();
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.close();
            System.out.println("close es success");
        }
    }

    /**
     * 查询全部文档，matchAllQuery
     */
    @Test
    public void testSearchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询索引中所有文档
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置返回条数（默认10，最多10000）
        sourceBuilder.size(100);

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("查询全部文档:" + searchResponse);

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
        System.out.println("查询全部文档：一共" + users.size() + "条数据");

    }

    /**
     * 搜索精确匹配，termQuery
     * name = "韩立"
     */
    @Test
    public void testAccurateSearchyName() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // name 是 text 类型：用于全文搜索，会分词
        // sourceBuilder.query(QueryBuilders.termQuery("name", "韩"));

        // name.keyword 是 keyword 类型：用于精确匹配，不分词，存储完整字符串
        sourceBuilder.query(QueryBuilders.termQuery("name.keyword", "韩立"));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("根据名称精确查询文档:" + searchResponse);

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
        System.out.println("根据名称精确查询文档：一共" + users.size() + "条数据");
    }

    /**
     * 模糊查询，matchQuery
     */
    @Test
    public void testFuzzySearchyName() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name", "韩"));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("根据名称模糊查询文档:" + searchResponse);

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
        System.out.println("根据名称模糊查询文档：一共" + users.size() + "条数据");
    }

    /**
     * 模糊查询，fuzzyQuery
     */
    @Test
    public void testFuzzySearchyName2() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.fuzzyQuery("name", "韩"));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("根据名称模糊查询文档:" + searchResponse);

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
        System.out.println("根据名称模糊查询文档：一共" + users.size() + "条数据");
    }

    /**
     * 分页查询， from + size
     */
    @Test
    public void testPageSearchByName() throws IOException {
        int pageNum = 1;
        int pageSize = 2;

        // 1. 创建 SearchRequest
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        // 2. 构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 根据name模糊查询
        sourceBuilder.query(QueryBuilders.matchQuery("name", "林"));

        // 3. 分页设置
        int from = (pageNum - 1) * pageSize;
        // 起始位置
        sourceBuilder.from(from);
        // 每页数量
        sourceBuilder.size(pageSize);

        // 4. 排序（可选）
        sourceBuilder.sort("id", SortOrder.ASC);

        searchRequest.source(sourceBuilder);

        // 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("分页查询文档:" + searchResponse);
        System.out.println("分页查询文档：一共" + searchResponse.getHits().getTotalHits() + "条数据");

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
    }

    /**
     * 过滤返回字段，fetchSource
     */
    @Test
    public void testSearchFilterField() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name", "韩"));
        // 过滤返回字段
        String[] includes = new String[]{"id", "name", "age"};
        String[] excludes = {};
        sourceBuilder.fetchSource(includes, excludes);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("过滤返回字段:" + searchResponse);

        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
        System.out.println("过滤返回字段：一共" + users.size() + "条数据");
    }

    /**
     * 组合查询：boolQuery
     * name = "韩立" and age = 35
     */
    @Test
    public void testCombinationSearch_and() throws IOException {
        // 1. 创建搜索请求
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        // 2. 构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 使用boolQuery实现组合查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // name 精确匹配
        boolQueryBuilder.must(QueryBuilders.termQuery("name.keyword", "韩立"));
        // age 精确匹配
        boolQueryBuilder.must(QueryBuilders.termQuery("age", "35"));

        // 设置查询体
        sourceBuilder.query(boolQueryBuilder);
        searchRequest.source(sourceBuilder);

        // 3. 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("组合查询文档:" + searchResponse);
        System.out.println("组合查询文档：一共" + searchResponse.getHits().getTotalHits() + "条数据");
        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
    }

    /**
     * 组合查询：boolQuery
     * age = 32 or ange  = 18
     *
     * @throws IOException
     */
    @Test
    public void testCombinationSearch_or() throws IOException {
        // 1. 创建搜索请求
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        // 2. 构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 使用boolQuery实现组合查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 条件1：age = 32
        boolQueryBuilder.should(QueryBuilders.termQuery("age", "32"));
        // 条件2：age = 18
        boolQueryBuilder.should(QueryBuilders.termQuery("age", "18"));

        // 设置查询体
        sourceBuilder.query(boolQueryBuilder);
        searchRequest.source(sourceBuilder);

        // 3. 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("组合查询文档:" + searchResponse);
        System.out.println("组合查询文档：一共" + searchResponse.getHits().getTotalHits() + "条数据");
        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
    }

    /**
     * 范围查询：age > 30 and age <= 40
     */
    @Test
    public void testRangeQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        // 使用rangeQuery实现范围查询,age > 30 and age <= 40
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age")
                .gt(30)
                .lte(40);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(rangeQueryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("范围查询文档:" + searchResponse);
        System.out.println("范围查询文档：一共" + searchResponse.getHits().getTotalHits() + "条数据");
        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
    }

    /**
     * 查询 name 包含 "路" 的用户，并高亮显示 HighlightBuilder
     */
    @Test
    public void testSearchWithHighLight() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 1. 查询条件：name 包含 "路"
        sourceBuilder.query(QueryBuilders.matchQuery("name", "路"));

        // 2. 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        // 指定要高亮的字段
        highlightBuilder.field("name");

        // 可选：自定义高亮标签
        highlightBuilder.preTags("<em>");  // 前置标签
        highlightBuilder.postTags("</em>"); // 后置标签

        // 可选：设置 fragment 大小（高亮片段长度）
        // highlightBuilder.fragmentSize(50);

        // 将高亮配置添加到查询中
        sourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(sourceBuilder);

        // 3. 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("高亮设置文档:" + searchResponse);
        System.out.println("高亮设置文档：一共" + searchResponse.getHits().getTotalHits() + "条数据");
        List<User> users = parseUsers(searchResponse.getHits());
        System.out.println(users);
    }

    /**
     * 聚合查询：查询年龄最大的用户
     */
    @Test
    public void testMaxAge() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //  1. 先用 max 聚合找出最大年龄
        String ageFieldName = "age";
        String maxAgeName = "max_age";
        AggregationBuilder aggregationBuilder = AggregationBuilders.max(maxAgeName).field(ageFieldName);
        sourceBuilder.aggregation(aggregationBuilder);

        // 2. 用 top_hits 聚合获取年龄最大那条文档的完整信息
        String oldestUserName = "oldest_user";
        AggregationBuilder aggregationBuilder2 = AggregationBuilders.topHits(oldestUserName).size(1).sort(ageFieldName, SortOrder.DESC);
        sourceBuilder.aggregation(aggregationBuilder2);

        // 注意：我们不需要返回原始匹配的文档，所以可以禁用
        sourceBuilder.size(0);

        searchRequest.source(sourceBuilder);

        // 3. 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("聚合查询，查询年龄最大的用户:" + searchResponse);

        // 4.解析聚合结果
        Max maxAgeAgg = searchResponse.getAggregations().get(maxAgeName);
        double maxAge = maxAgeAgg.getValue(); // 最大年龄
        System.out.println("最大年龄：" + maxAge);

        ParsedTopHits topHits = searchResponse.getAggregations().get(oldestUserName);
        List<User> users = parseUsers(topHits.getHits());
        System.out.println("年龄最大的用户：" + users.get(0));
    }

    /**
     * 聚合查询：查询年龄分组，并统计每个年龄的用户数量
     */
    @Test
    public void testGroupByAgeAndCount() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        // 构建 terms 聚合：按 age 字段分组，统计数量
        String ageFieldName = "age";
        String aggGroupByAge = "age_group";
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms(aggGroupByAge)
                .field(ageFieldName)
                // 默认只返回10个分组
                .size(1000)
                // 按年龄升序
                .order(BucketOrder.key(true));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 可以不返回原始文档
        sourceBuilder.size(0);
        sourceBuilder.aggregation(aggregationBuilder);

        searchRequest.source(sourceBuilder);

        // 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("聚合查询，查询年龄分组，并统计每个年龄的用户数量:" + searchResponse);

        // 解析聚合结果
        ParsedTerms groupByAge = searchResponse.getAggregations().get(aggGroupByAge);
        for (Terms.Bucket bucket : groupByAge.getBuckets()) {
            int age = ((Number) bucket.getKey()).intValue();
            long userCount = bucket.getDocCount();

            System.out.printf("年龄: %2d 岁 -> 用户数量: %d 人%n", age, userCount);
        }
    }

    private List<User> parseUsers(SearchHits hits) throws JsonProcessingException {
        List<User> users = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            users.add(objectMapper.readValue(hit.getSourceAsString(), User.class));
        }
        return users;
    }

}
