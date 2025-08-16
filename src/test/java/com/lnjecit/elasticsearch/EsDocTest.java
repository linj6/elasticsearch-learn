package com.lnjecit.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lnjecit.elasticsearch.domain.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EsDocTest {
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
    public void testCreateDoc() throws Exception {
        IndexRequest request = new IndexRequest(USER_INDEX);
        Long id = 1L;
        request.id(id.toString());

        User user = new User();
        user.setId(id);
        user.setName("王林");
        user.setAge(28);
        user.setSex("男");

        String json = objectMapper.writeValueAsString(user);
        request.source(json, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("创建文档:" + objectMapper.writeValueAsString(response));
    }

    @Test
    public void testGetDoc() throws IOException {
        GetRequest request = new GetRequest(USER_INDEX, "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("查询文档结果:" + response.getSourceAsString());
    }

    @Test
    public void testUpdateDoc() throws IOException {
        Long id = 1L;
        User user = new User();
        user.setId(id);
        user.setName("王林");
        user.setAge(32);
        user.setSex("男");

        UpdateRequest request = new UpdateRequest(USER_INDEX, id.toString());
        request.doc(objectMapper.writeValueAsString(user), XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println("更新文档结果:" + response);
    }

    @Test
    public void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest(USER_INDEX, "1");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("删除文档结果:" + response);
    }

    @Test
    public void testBatchCreateDoc() throws IOException {
        List<User> users = prepareUsers();

        BulkRequest bulkRequest = new BulkRequest();
        for (User user : users) {
            IndexRequest indexRequest = new IndexRequest(USER_INDEX);
            indexRequest.id(user.getId().toString());
            indexRequest.source(objectMapper.writeValueAsString(user), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("批量创建文档结果:" + bulkResponse.getItems().length);
    }

    @Test
    public void testBatchDeleteDoc() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        List<String> docIds = new ArrayList<>(Arrays.asList("2", "3", "4"));
        for (String docId : docIds) {
            DeleteRequest deleteRequest = new DeleteRequest(USER_INDEX, docId);
            bulkRequest.add(deleteRequest);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("批量删除文档结果:" + bulkResponse.getItems().length);
    }

    private List<User> prepareUsers() {
        List<User> users = new ArrayList<>();
        User user1 = new User(1L, "王林", 32, "男");
        User user2 = new User(2L, "韩立", 35, "男");
        User user3 = new User(3L, "紫川秀", 22, "男");
        User user4 = new User(4L, "张小凡", 25, "男");
        User user5 = new User(5L, "路明非", 19, "男");
        User user6 = new User(6L, "楚子航", 20, "男");
        User user7 = new User(7L, "韩非", 33, "男");
        User user8 = new User(8L, "李沐婉", 22, "女");
        User user9 = new User(9L, "紫灵", 18, "女");
        User user10 = new User(10L, "张楚岚", 19, "男");
        User user11 = new User(11L, "王也", 20, "男");
        User user12 = new User(12L, "张之维", 100, "男");
        User user13 = new User(13L, "路明泽", 15, "男");
        User user14 = new User(14L, "萧炎", 22, "男");
        User user15 = new User(15L, "林动", 20, "男");
        User user16 = new User(16L, "牧尘", 19, "男");
        User user17 = new User(17L, "叶凡", 28, "男");
        User user18 = new User(18L, "石昊", 16, "男");
        User user19 = new User(19L, "秦羽", 25, "男");
        User user20 = new User(20L, "楚南", 24, "男");
        User user21 = new User(21L, "方寒", 21, "男");
        User user22 = new User(22L, "李七夜", 3000, "男");
        User user23 = new User(23L, "苏白衣", 18, "男");
        User user24 = new User(24L, "陈长生", 17, "男");
        User user25 = new User(25L, "徐凤年", 23, "男");
        User user26 = new User(26L, "李淳罡", 80, "男");
        User user27 = new User(27L, "邓布利多", 150, "男");
        User user28 = new User(28L, "宁缺", 19, "男");
        User user29 = new User(29L, "桑桑", 18, "女");
        User user30 = new User(30L, "余帘", 20, "女");
        User user31 = new User(31L, "莫山山", 19, "女");
        User user32 = new User(32L, "叶玄", 22, "男");
        User user33 = new User(33L, "陆尘", 28, "男");
        User user34 = new User(34L, "南宫流云", 26, "男");
        User user35 = new User(35L, "百里东君", 24, "男");
        User user36 = new User(36L, "司空长风", 25, "男");
        User user37 = new User(37L, "雷无桀", 18, "男");
        User user38 = new User(38L, "唐三", 20, "男");
        User user39 = new User(39L, "小舞", 18, "女");
        User user40 = new User(40L, "胡列娜", 22, "女");
        User user41 = new User(41L, "比比东", 45, "女");
        User user42 = new User(42L, "波塞冬", 300, "男");
        User user43 = new User(43L, "赵敏", 20, "女");
        User user44 = new User(44L, "周芷若", 21, "女");
        User user45 = new User(45L, "小龙女", 25, "女");
        User user46 = new User(46L, "黄蓉", 23, "女");
        User user47 = new User(47L, "郭靖", 25, "男");
        User user48 = new User(48L, "杨过", 22, "男");
        User user49 = new User(49L, "张无忌", 24, "男");
        User user50 = new User(50L, "令狐冲", 26, "男");
        User user51 = new User(51L, "东方不败", 28, "男");
        User user52 = new User(52L, "岳不群", 45, "男");
        User user53 = new User(53L, "任盈盈", 21, "女");
        User user54 = new User(54L, "独孤求败", 80, "男");
        User user55 = new User(55L, "风清儿", 19, "女");
        User user56 = new User(56L, "姬如雪", 20, "女");
        User user57 = new User(57L, "颜庄", 23, "女");
        User user58 = new User(58L, "燕赤霞", 35, "男");
        User user59 = new User(59L, "宁采臣", 24, "男");
        User user60 = new User(60L, "聂小倩", 18, "女");
        User user61 = new User(61L, "白素贞", 28, "女");
        User user62 = new User(62L, "许仙", 22, "男");
        User user63 = new User(63L, "法海", 60, "男");
        User user64 = new User(64L, "沈浪", 25, "男");
        User user65 = new User(65L, "李寻欢", 30, "男");
        User user66 = new User(66L, "楚留香", 28, "男");
        User user67 = new User(67L, "陆小凤", 27, "男");
        User user68 = new User(68L, "花满楼", 29, "男");
        User user69 = new User(69L, "西门吹雪", 31, "男");
        User user70 = new User(70L, "傅红雪", 26, "男");
        User user71 = new User(71L, "叶开", 23, "男");
        User user72 = new User(72L, "谢晓峰", 35, "男");
        User user73 = new User(73L, "燕十三", 33, "男");
        User user74 = new User(74L, "阿飞", 21, "男");
        User user75 = new User(75L, "林诗音", 26, "女");
        User user76 = new User(76L, "孙小红", 22, "女");
        User user77 = new User(77L, "上官金虹", 40, "男");
        User user78 = new User(78L, "怜星", 45, "女");
        User user79 = new User(79L, "邀月", 48, "女");
        User user80 = new User(80L, "江玉燕", 20, "女");
        User user81 = new User(81L, "花无缺", 24, "男");
        User user82 = new User(82L, "小鱼儿", 22, "男");
        User user83 = new User(83L, "铁心兰", 21, "女");
        User user84 = new User(84L, "苏樱", 20, "女");
        User user85 = new User(85L, "魏无羡", 20, "男");
        User user86 = new User(86L, "蓝忘机", 22, "男");
        User user87 = new User(87L, "江澄", 21, "男");
        User user88 = new User(88L, "金光瑶", 35, "男");
        User user89 = new User(89L, "薛洋", 18, "男");
        User user90 = new User(90L, "晓星尘", 25, "男");
        User user91 = new User(91L, "宋岚", 24, "男");
        User user92 = new User(92L, "蓝曦臣", 30, "男");
        User user93 = new User(93L, "温宁", 20, "男");
        User user94 = new User(94L, "温情", 28, "女");
        User user95 = new User(95L, "聂明玦", 38, "男");
        User user96 = new User(96L, "贺玄", 27, "男");
        User user97 = new User(97L, "齐夏", 19, "男");
        User user98 = new User(98L, "乔家劲", 22, "男");
        User user99 = new User(99L, "柳卿卿", 23, "女");
        User user100 = new User(100L, "沈清秋", 29, "男");

        users.add(user1);
        users.add(user2);
        users.add(user3);
        users.add(user4);
        users.add(user5);
        users.add(user6);
        users.add(user7);
        users.add(user8);
        users.add(user9);
        users.add(user10);
        users.add(user11);
        users.add(user12);
        users.add(user13);
        users.add(user14);
        users.add(user15);
        users.add(user16);
        users.add(user17);
        users.add(user18);
        users.add(user19);
        users.add(user20);
        users.add(user21);
        users.add(user22);
        users.add(user23);
        users.add(user24);
        users.add(user25);
        users.add(user26);
        users.add(user27);
        users.add(user28);
        users.add(user29);
        users.add(user30);
        users.add(user31);
        users.add(user32);
        users.add(user33);
        users.add(user34);
        users.add(user35);
        users.add(user36);
        users.add(user37);
        users.add(user38);
        users.add(user39);
        users.add(user40);
        users.add(user41);
        users.add(user42);
        users.add(user43);
        users.add(user44);
        users.add(user45);
        users.add(user46);
        users.add(user47);
        users.add(user48);
        users.add(user49);
        users.add(user50);
        users.add(user51);
        users.add(user52);
        users.add(user53);
        users.add(user54);
        users.add(user55);
        users.add(user56);
        users.add(user57);
        users.add(user58);
        users.add(user59);
        users.add(user60);
        users.add(user61);
        users.add(user62);
        users.add(user63);
        users.add(user64);
        users.add(user65);
        users.add(user66);
        users.add(user67);
        users.add(user68);
        users.add(user69);
        users.add(user70);
        users.add(user71);
        users.add(user72);
        users.add(user73);
        users.add(user74);
        users.add(user75);
        users.add(user76);
        users.add(user77);
        users.add(user78);
        users.add(user79);
        users.add(user80);
        users.add(user81);
        users.add(user82);
        users.add(user83);
        users.add(user84);
        users.add(user85);
        users.add(user86);
        users.add(user87);
        users.add(user88);
        users.add(user89);
        users.add(user90);
        users.add(user91);
        users.add(user92);
        users.add(user93);
        users.add(user94);
        users.add(user95);
        users.add(user96);
        users.add(user97);
        users.add(user98);
        users.add(user99);
        users.add(user100);
        return users;
    }
}
