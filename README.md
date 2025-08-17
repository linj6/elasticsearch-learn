## Elasticsearch 学习项目

这是一个用于学习和实践 Elasticsearch 操作的 Java 项目，使用 Elasticsearch High Level REST Client 进行各种索引、文档和搜索操作。

### 项目结构

```
.
├── main
│   ├── java
│   │   └── com.lnjecit.elasticsearch.domain
│   │       └── User.java              # User 实体类
│   └── resources
│       └── log4j2.xml                 # 日志配置文件
└── test
    └── java
        └── com.lnjecit.elasticsearch
            ├── EsClientTest.java      # ES 客户端连接测试
            ├── EsDocSearchTest.java   # 文档搜索操作测试
            ├── EsDocTest.java         # 文档增删改查操作测试
            └── EsIndexTest.java       # 索引操作测试
```


### 功能模块

#### 1. 实体类 - User

#### 2. 索引操作 (EsIndexTest)
- 创建索引
- 检查索引是否存在
- 删除索引
- 获取索引信息

#### 3. 文档操作 (EsDocTest)
- 创建单个文档
- 批量创建文档 (100个用户数据)
- 查询文档
- 更新文档
- 删除文档
- 批量删除文档

#### 4. 搜索操作 (EsDocSearchTest)
- 全文搜索 (`matchAllQuery`)
- 精确匹配搜索 (`termQuery`)
- 模糊搜索 (`matchQuery`, `fuzzyQuery`)
- 分页搜索
- 字段过滤
- 组合查询 (`boolQuery`)
- 范围查询 (`rangeQuery`)
- 高亮显示
- 聚合查询 (最大值、分组统计)

### 环境要求

- Java 8+
- Elasticsearch 7.x
- Maven

### 使用方法

1. 确保 Elasticsearch 服务在本地 9200 端口运行
2. 克隆项目到本地
3. 运行各个测试类中的测试方法来体验不同功能

### 主要依赖

```xml
<dependency>
    <groupId>org.elasticsearch.client</groupId>
    <artifactId>elasticsearch-rest-high-level-client</artifactId>
    <version>7.x.x</version>
</dependency>
```


### 测试数据

项目中包含100个预定义的用户数据，用于测试各种 Elasticsearch 功能。