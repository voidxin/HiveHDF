package com.zhangxin.hive;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.io.Text;

public class PassAge extends UDF {

    public Text evaluate(Integer age)

    {

        Text result = new Text();

       

        if(age > 30)

            result.set("morethan30");

        else

            result.set("lessthan30");

       

        return result;    

    }

}