package com.xiaokan.test;

import java.awt.BorderLayout;
import java.nio.charset.Charset;

import javax.swing.JFrame;

import com.csvreader.CsvReader;

public class TestCsvReader {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		JFrame f = new JFrame();
		CsvReader fin = new CsvReader("E:/ddd.csv", ',', Charset.forName("UTF-8"));
		fin.setSafetySwitch(false);
		while (fin.readRecord()) {
			String[] values = fin.getValues();
			System.out.println(values[0] + "," + values[1].length());
		}
	}

}
