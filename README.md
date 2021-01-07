## 读取hadoop中的数据
1. 启动hadoop
2. 读取hdfs中的数据，
JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/allen/works_album_info.txt");

注： works_album_info.txt 中有 151条记录, 23个字段。

## 关于 album_works_info.txt 
* 1 input/works_album_info.txt 文件内容，是在测试环境运行如下 slq 得到的。
select id,name,name_en,subtitle,category,cluster_id,external_id,site_id,state,sys_add,album_type,screen_year,screen_time,director_name,actor_name,area_name,sub_category_name,language_name,tags_name,is_pay,create_time,update_time,img from works_album_info where id < 1000;

* 2 入口机hive中 temp db里面创建 works_album_info.

1）创建表：
```sql
create table if not exists temp.works_album_info(
                           id INT,
                           name STRING,
                           name_en STRING,
                           subtitle STRING,
                           category TINYINT,
                           cluster_id INT,
                           external_id STRING,
                           site_id INT,
                           state TINYINT,
                           sys_add TINYINT,
                           album_type TINYINT,
                           screen_year INT,
                           screen_time STRING,
                           director_name STRING,
                           actor_name STRING,
                           area_name STRING,
                           sub_category_name STRING,
                           language_name STRING,
                           tags_name STRING,
                           is_pay TINYINT,
                           create_time TIMESTAMP,
                           update_time TIMESTAMP,
                           img STRING
                           ) 
                           COMMENT 'album info data' 
                           ROW FORMAT DELIMITED 
                           FIELDS TERMINATED BY '\t' 
                           LINES TERMINATED BY '\n' 
                           STORED AS TEXTFILE
                           TBLPROPERTIES ('creator'='hubo7','created_at'='2020-03-12 13:00:00');
```
2）从 input/works_album_info.txt 文件导入数据。
```sql
 load data local inpath '/home/hubo7/AlbumCount/works_album_info.txt' into table works_album_info;
 ```



