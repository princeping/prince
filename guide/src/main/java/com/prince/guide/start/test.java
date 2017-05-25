package com.prince.guide.start;

import jodd.datetime.JDateTime;

public class test {

	public static void main(String[] args) {
		JDateTime jDateTime = new JDateTime();
		System.out.println(jDateTime.toString("YYYYMMDD"));

	}

}
