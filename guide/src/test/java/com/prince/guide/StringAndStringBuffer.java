package com.prince.guide;

public class StringAndStringBuffer {

	public static void main(String[] args) {
		
		long starttime1 = System.currentTimeMillis();
		String s1 = "1" + "+1" + "=2";
		System.out.println("计算第一个" + s1 + "耗时" + (System.currentTimeMillis()-starttime1)*100000000);

		long starttime2 = System.currentTimeMillis();
		StringBuffer s2 = new StringBuffer("1").append("+1").append("=2");
		System.out.println("计算第二个" + s2 + "耗时" + (System.currentTimeMillis()-starttime2)*100000000);
	}

}
