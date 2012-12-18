package com.github.diaosi.BDAM.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.csvreader.CsvWriter;

public class LangExtractor {

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {

		Document doc = Jsoup.parse(new FileInputStream("E:\\Datasets\\Wikipedia\\lang.txt"), "UTF-8", "");
		CsvWriter fout = new CsvWriter("E:\\Datasets\\Wikipedia\\language_table.csv", ',', Charset.forName("UTF-8"));
		Element table = doc.select("table").first();
		for (Element child : table.child(1).children()) {
			Elements tds = child.children();
			
			// Color
			Element color_td = tds.get(0);
			String color = color_td.attr("bgcolor");
			
			// language family
			Element lf = tds.get(1).child(0);
			String lang_family = lf.ownText();
			
			// language name
			Element ln = tds.get(2).child(0);
			String name = ln.ownText();
			
			// short name
			Element sn = tds.get(4);
			String short_name = sn.ownText();
		
			fout.writeRecord(new String[] { color, lang_family, name, short_name });
		}
		fout.flush();
		fout.close();

	}
	
}
