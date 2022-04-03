# geek-week5
## 建表并导入数据
```sql
-- 建表
create table t_user_zanglei (
userid INT,
sex STRING,
age INT,
occupation STRING,
zipcode STRING
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'='::');
-- 导入数据
load data local inpath '/data/hive/users.dat' overwrite into table t_user_zanglei;
-- 查询
select userid, sex, age, occupation, zipcode from t_user_zanglei limit 10;

create table t_movie_zanglei (
movieid INT,
moviename STRING,
movietype STRING
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'='::');

load data local inpath '/data/hive/movies.dat' overwrite into table t_movie_zanglei;

select movieid, moviename, movietype from t_movie_zanglei limit 10;

create table t_rating_zanglei (
userid INT,
movieid INT,
rate INT,
times INT
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'='::');

load data local inpath '/data/hive/ratings.dat' overwrite into table t_rating_zanglei;

select userid, movieid, rate, times from t_rating_zanglei limit 10;
```

## 第一题
```sql
select u.age, avg(r.rate) from t_user_zanglei u left join t_rating_zanglei r on u.userid = r.userid where movieid = 2116 group by u.age;
```

## 第二题
```sql
select u.sex, m.moviename, avg(r.rate), count(u.userid) as total 
from t_user_zanglei u left join t_rating_zanglei r on u.userid = r.userid 
left join t_movie_zanglei m on r.movieid = m.movieid 
where u.sex = 'M' group by m.moviename, u.sex having count(u.userid) > 50 order by avg(r.rate) desc  limit 10;
```

## 第三题
第一个子查询找到评分最多的女士，  
第二个子查询查询她给出评分最高的电影，由于她给5分的电影超过10部，所以用电影名排序取前10部
```sql
select m.moviename, avg(r.rate) from t_movie_zanglei m 
left join t_rating_zanglei r on m.movieid = r.movieid
where m.movieid in 
  (select r.movieid from t_rating_zanglei r 
left join t_movie_zanglei m on r.movieid = m.movieid 
  where r.userid = 
    (select u.userid
      from t_rating_zanglei r 
      left join t_user_zanglei u 
        on u.userid = r.userid 
    where u.sex = 'F' 
    group by u.userid order by count(u.userid) desc limit 1)
  order by r.rate desc, m.moviename limit 10
  )
group by m.moviename;
```