package com.github.diaosi.BDAM.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestREIndex {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "{{Infobox company\n|  company_name   = American Media, Inc.";
		Pattern pat = Pattern.compile("[\\|\\}\\n\\r]");
		Matcher m = pat.matcher(str);
		m.find();
		System.out.println(m.start());

	}

}
