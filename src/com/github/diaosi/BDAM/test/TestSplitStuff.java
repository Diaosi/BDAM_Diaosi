package com.github.diaosi.BDAM.test;

import java.util.Arrays;
import java.util.regex.Pattern;

public class TestSplitStuff {

	/**
	 * @param args
	 */
	private static String pat = "[\\~\\`\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\_"
			+ "\\-\\+\\=\\|\\\\\\\\}\\]\\{\\[\\:\\;"
			+ "\\'\\\\\"\\?\\/\\>\\.\\<\\,\\ \\–\\’\\\t\\n\\r\\f"
			+ "\\\342\\\200\\\216\\\224]";
	private static Pattern pattern = Pattern.compile(pat);

	public static void main(String[] args) {

		String str = "a,b,c,";
		System.out.println(Arrays.toString(str.split(",")));
		System.out.println(str.split(",").length);
		String sss = "ab|1,23|2,d8|3";
		System.out.println(Arrays.toString(sss.split("\\|")));
		System.out.println(sss.split("\\|").length);
		String ssss = "a\ta\tb\tb\f01\342s23";
		System.out.println(Arrays.toString(pattern.split(ssss)));
		System.out.println(pattern.split(ssss).length);
		System.out.println(Arrays.toString(ssss.split("[^A-Za-z0-9]+")));


	}
}
