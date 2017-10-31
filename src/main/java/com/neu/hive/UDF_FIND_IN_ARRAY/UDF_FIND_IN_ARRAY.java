package com.neu.hive.UDF_FIND_IN_ARRAY;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/*
* 2.比较HQL中的order by;sort by;cluster by;distribute by的作用，并分别尝试使用它们。
3.一下二选一。
实现一个UDF去查找hive表中array类型列的值中是否包含某一项。
SELECT FIND_IN_ARRAY(列名, 搜索字符) FROM users;
实现一个UDF去链接Array字段的值。
SELECT ARRAY_CONTACT(列名, 搜索字符) FROM users;
4.学习UDAF，并尝试使用UDAF去实现TOPK算法。
*/
public class UDF_FIND_IN_ARRAY extends UDF{
    List<String> list=new ArrayList<String>();
    public String evaluate(ArrayList<String> arrayList,String str){

        Iterator<String> it=arrayList.iterator();
        while(it.hasNext()){
            String string=it.next();
            if (str==string){
                list.add(str);

            }
        }
        return list.toString();
    }
}
