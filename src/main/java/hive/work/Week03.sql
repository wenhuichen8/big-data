登录链接： http://knox.c-01b94588f59c7655.cn-hangzhou.emr.aliyuncs.com:8888/hue

# 第三周作业==========

## 1、说明
使用EMR平台的 HUE 跑 HiveQL；

## 2、准备工作

- 1、创建database

```
create database chenwenhui;
```

- 2、创建表之前查看数据结构
```
cd /data/hive
cat users.dat
```

- 3、数据是以::分隔，需使用多分隔符进行切分；在建表前将数据copy到hdfs中
```
#创建目录
hdfs dfs -mkdir -p /chenwenhu/hive/week03/users
hdfs dfs -mkdir -p /chenwenhu/hive/week03/movies
hdfs dfs -mkdir -p /chenwenhu/hive/week03/ratings

#将本地文件提交到hdfs
hdfs dfs -put   /data/hive/users.dat   /chenwenhu/hive/week03/users
hdfs dfs -put   /data/hive/movies.dat   /chenwenhu/hive/week03/movies
hdfs dfs -put   /data/hive/ratings.dat   /chenwenhu/hive/week03/ratings

#查看是否拷贝正确
hdfs dfs -ls   /chenwenhu/hive/week03/users
hdfs dfs -ls   /chenwenhu/hive/week03/movies
hdfs dfs -ls   /chenwenhu/hive/week03/ratings
```

- 4、文件拷贝到hdfs之后，可以建表，建表语句如下：

```
#创建t_user表，由于是外部表，建表的同时会load数据
create external table t_user(
UserID string, Sex string, Age int,
Occupation int, Zipcode int
)
--通过自定义的SerDe或使用Hive内置的SerDe类型指定数据的序列化和反序列化方式
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::") 
LOCATION "/chenwenhu/hive/week03/users";

#创建t_movie表，由于是外部表，建表的同时会load数据
create external table t_movie(
MovieID string, MovieName string, MovieType string
)
--通过自定义的SerDe或使用Hive内置的SerDe类型指定数据的序列化和反序列化方式
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::") 
LOCATION "/chenwenhu/hive/week03/movies";

#创建t_rating表，由于是外部表，建表的同时会load数据
create external table t_rating(
UserID string, MovieID string, Rate string,Times float
)
--通过自定义的SerDe或使用Hive内置的SerDe类型指定数据的序列化和反序列化方式
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::") 
LOCATION "/chenwenhu/hive/week03/ratings";
```
- 5、查看数据

```
select * from t_user;
select * from t_movie;
select * from t_rating;
```
##  作业1

```
--展示电影 ID 为 2116 这部电影各年龄段的平均影评分
select u.age,avg(r.rate) avg
from t_rating r,t_user u
where r.userid=u.userid
 and r.movieid='2116'
group by u.age;
```
##  作业2

```
--找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。
/*男性评分最高且评分次数超过 50 次的 10 部电影*/     
select m2.moviename,avg(r2.rate) avgrate,count(1) total
 from t_rating r2,
      t_user u2,
      t_movie m2,
     (select distinct r.movieid  /**男性评分最高**/
      from t_rating r,
           t_user u,
           (select r.movieid,max(r.rate) maxrate
            from  t_rating r
            group by r.movieid) rt
      where r.userid=u.userid
       and r.movieid=rt.movieid
       and r.rate=rt.maxrate
       and u.sex='M') tt
where r2.movieid=tt.movieid
  and r2.userid=u2.userid
  and r2.movieid=m2.movieid
  and u2.sex='M'    /**男性**/
  --and m2.movieid ='2905'  /**用于数据验证**/
group by m2.moviename
having count(1)>50  /**男性评价超过50 **/
limit 10;
```
##  作业3

```
/*找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）*/
with tb_ratemax as /*影评次数最多的女士信息*/
  (select u.userid,count(1) ratecount  
    from t_user u,
         t_rating r
    where u.userid=r.userid
      and u.sex='F'
    group by u.userid
    order by ratecount desc
    limit 1),
  tb_movieinfo as   /*该女士所给分数为最高分的 10 部电影*/
  (select r.movieid  
    from tb_ratemax u,
         t_rating r
    where u.userid=r.userid
      and exists(
        select * 
        from(
           select r1.movieid,max(r1.rate) maxrate
           from t_rating r1
           group by r1.movieid) t1
        where t1.maxrate=r.rate
          and t1.movieid=r.movieid
      )
      limit 10
   )
  /*最终结果： 10 部电影的 电影名和平均影评分*/ 
  select m.moviename,avg(r.rate) rate
  from tb_movieinfo a,
       t_movie m,
       t_rating r
  where a.movieid=m.movieid
   and  a.movieid=r.movieid
  group by m.moviename;
```
执行结果：
![image](https://user-images.githubusercontent.com/62194796/170172498-97b8250b-0078-4a7f-92a4-de98e0f3c44e.png)
