package com.zhangxin.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
@SuppressWarnings("deprecation")
public class MaxAge extends UDAF {
     public static class MaxAgeIntUDAFEvaluator implements UDAFEvaluator {
        //最终结果
    	private IntWritable result;
		@Override
		public void init() {
			// TODO Auto-generated method stub
			result = null;
		}
		//每次对一个新值进行聚集计算都会调用iterate方法
		public boolean iterate(IntWritable age) {
			if (age == null)
				return false;
			if (result == null) 
				result = new IntWritable(age.get());
			else
				result.set(Math.max(result.get(), age.get()));
			return true;
		}
		//hive需要部分聚集结果的时候会调用该方法，会返回一个封装了聚集计算当前状态的对象
		public IntWritable terminatePartial() {
			
			return result;
		}
		
		//合并两个部分聚集值会调用这个方法
		public boolean merge(IntWritable other) {
			return iterate(other);
		}
		
		//hive需要最终聚集结果时候会调用该方法
		public IntWritable terminate() {
			return result;
		}
    	 
     }
}
