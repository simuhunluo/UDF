package com.neu.hive.UDF;
/*
*
* add jar /usr/local/usrJars/dulm/hiveUDF-0.0.1-SNAPSHOT-all.jar; 在hive中执行把jar
create temporary function my_uppercase as 'com.neu.hive.UDF.ToUpperCaseUDF';
创建      临时的     方法     叫做my_uppercase   as  你的包名+类名
 select my_uppercase(datasource) from tenmindata limit 10;
 测试使用： 选出字段datasource下的数据并全部转为大写，显示前10条。
* */