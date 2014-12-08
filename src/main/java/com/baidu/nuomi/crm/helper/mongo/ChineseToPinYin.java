package com.baidu.nuomi.crm.helper.mongo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class ChineseToPinYin {
	/*
	 * 汉字转换为urlencoder编码
	 */
	public static String chineseToPinyinWithPlus(String chinese) {
		chinese = toDBC(chinese);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < chinese.length(); i++) {
			String ss = chinese.substring(i, i + 1);
			// 如果是汉字
			if (ss.matches("[\u4e00-\u9fa5]")) {

				try {
					// 变成urlencoder编码
					sb.append(URLEncoder.encode(ss, "utf-8")
							.replaceAll("%", ""));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				sb.append(' ');

			} else if(ss.matches("[A-Za-z]")){
				ss=ss.toLowerCase();
				sb.append(printBits((int)ss.charAt(0)));
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}else if(ss.matches("[\\d]")){
				sb.append(printBits(Integer.valueOf(ss)));
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}else
			{
				sb.append(ss);
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}
		}
		return "\"" + sb.toString() + "\"";
	}

	/**/// /
	// / 转半角 的函数(DBC case)
	// /
	// / 任意字符串
	// /半角 字符串
	// /
	// /全角 空格为12288，半角 空格为32
	// /其他字符半角 (33-126)与全角 (65281-65374)的对应关系是：均相差65248
	// /
	public static String toDBC(String input) {
		char[] c = input.toCharArray();
		for (int i = 0; i < c.length; i++) {
			if (c[i] == 12288) {
				c[i] = (char) 32;
				continue;
			}
			if (c[i] > 65280 && c[i] < 65375)
				c[i] = (char) (c[i] - 65248);
		}
		return new String(c);
	}

	public static String chineseToPinyinWithOutPlus(String chinese) {
		chinese = toDBC(chinese);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < chinese.length(); i++) {
			String ss = chinese.substring(i, i + 1);
			// 如果是汉字
			if (ss.matches("[\u4e00-\u9fa5]")) {

				try {
					// 变成urlencoder编码
					sb.append(URLEncoder.encode(ss, "utf-8")
							.replaceAll("%", ""));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				sb.append(' ');

			} else if(ss.matches("[A-Za-z]")){
				ss=ss.toLowerCase();
				sb.append(printBits((int)ss.charAt(0)));
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}else if(ss.matches("[\\d]")){
				sb.append(printBits(Integer.valueOf(ss)));
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}else
			{
				sb.append(ss);
				if (i < chinese.length() - 1
						)
					sb.append(' ');
			}
		}
		return sb.toString();
	}
	public static StringBuffer printBits(int n){
		StringBuffer result = new StringBuffer();
		int temp = n;
		while(n!=0){
			temp =n%2;
			n =n/2;
			result.insert(0, temp);
		}
		if(result.length()<6){
			for(int i=0;i<6;i++){
				result.insert(0, '0');
			}
		}
		return result;
	}
	public static void main(String[] args) {
		chineseToPinyinWithOutPlus("17.5zz");
	}
}