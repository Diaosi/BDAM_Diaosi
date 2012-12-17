package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CountOfInfoboxes {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		BufferedReader fin = new BufferedReader(
				new InputStreamReader(
						new FileInputStream(
								"/e_drive/Datasets/Wikipedia/enwiki-latest-pages-articles.xml")),
				1024 * 1024);
		List<String> list = new ArrayList<String>();
		String line = null;
		long countOfInfoboxes = 0;
		long countOfLine = 0;
		long countOfNoTemplate = 0;
		long countOfPagesWithInfobox = 0;
		long countOfPages = 0;
		List<Integer> listOfPos = new ArrayList<Integer>();
		boolean flag = false;
		while ((line = fin.readLine()) != null) {
			line = line.trim();
			// list.add(line);
			if ("<page>".equals(line)) {
				countOfPages++;
				flag = false;
			}
			countOfLine++;
			if (countOfLine % 1000 == 0) {
				System.out.format("%dK lines\n", countOfLine / 1000);
			}
			if (line.startsWith("{{infobox")) {
				countOfInfoboxes++;
				flag = true;
			}
			if ("{{infobox".equals(line)) {
				countOfNoTemplate++;
				// listOfPos.add(list.size() - 1);
			}
			if ("</page>".equals(line) && flag) {
				countOfPagesWithInfobox++;
			}
		}
		// for (Integer pos : listOfPos) {
		// System.out.println("--------------------------------------------");
		// for (int i = pos - 5; i <= pos + 5; i++) {
		// System.out.println(list.get(i));
		// }
		// System.out.println("--------------------------------------------");
		// }
		System.out.format("Count of Lines: %d\n", countOfLine);
		System.out.format("Count of Infoboxes: %d\n", countOfInfoboxes);
		System.out.format("Count of Infoboxes without template: %d\n",
				countOfNoTemplate);
		System.out.format("Count of Pages with Infobox: %d\n",
				countOfPagesWithInfobox);
		System.out.format("Count of Pages: %d\n", countOfPages);
	}
}
