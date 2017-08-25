package com.zhangxin.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
@SuppressWarnings("deprecation")
public class NameMaxLength  extends UDAF{
	 //找出名字最长的
	 public static class NameMaxLengthIntUDAFEvaluator implements UDAFEvaluator {
	        //最终结果
	    	private String result;
			@Override
			public void init() {
				// TODO Auto-generated method stub
				result = null;
			}
			//每次对一个新值进行聚集计算都会调用iterate方法
			public boolean iterate(String name) {
				if (name == null)
					return false;
				if (result == null) 
					result = name;
				else{
					int rLength = result.length();
					int nLength = name.length();
					if(rLength < nLength) {
						result = name;
					} 
				}
				return true;
			}
			//hive需要部分聚集结果的时候会调用该方法，会返回一个封装了聚集计算当前状态的对象
			public String terminatePartial() {
				
				return result;
			}
			
			//合并两个部分聚集值会调用这个方法
			public boolean merge(String other) {
				return iterate(other);
			}
			
			//hive需要最终聚集结果时候会调用该方法
			public String terminate() {
				return result;
			}
	    	 
	     }
}
