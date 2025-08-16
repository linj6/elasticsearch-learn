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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

    private List<User> parseUsers(SearchHits hits) throws JsonProcessingException {
        List<User> users = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            users.add(objectMapper.readValue(hit.getSourceAsString(), User.class));
        }
        return users;
    }

}
