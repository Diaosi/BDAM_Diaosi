package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.github.diaosi.BDAM.inputformat.XmlInputFormat;

public class InfoboxGroup {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private DocumentBuilderFactory dbFactory = DocumentBuilderFactory
				.newInstance();
		private DocumentBuilder dBuilder;
		private String prefix = "Infobox";
		private Pattern pat = Pattern.compile("[\\|\\}\\n\\r]");

		public Map() {
			super();
			try {
				dBuilder = dbFactory.newDocumentBuilder();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {

				InputSource is = new InputSource(new StringReader(
						value.toString()));

				Document doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("page");

				Node nNode = nList.item(0);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String infoBoxText = parseInfoBox(getTagValue("text",
							eElement));
					String id = getTagValue("title", eElement);
					if (infoBoxText != null) {
						try {
							infoBoxText = infoBoxText.trim();
							Matcher m = pat.matcher(infoBoxText);
							int pos;
							if (!m.find())
								pos = -1;
							else
								pos = m.start();
							infoBoxText = infoBoxText.substring(
									infoBoxText.indexOf(prefix)
											+ prefix.length(),
									pos == -1 ? infoBoxText.length() : pos)
									.trim();
							if (infoBoxText.length() > 0)
								output.collect(new Text(infoBoxText), new Text(
										id));
						} catch (Exception e) {
							System.err.println(infoBoxText);
							e.printStackTrace();
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private static String getTagValue(String sTag, Element eElement) {
			NodeList nList = eElement.getElementsByTagName(sTag).item(0)
					.getChildNodes();
			Node nValue = nList.item(0);
			return nValue.getNodeValue();
		}

		private static String parseInfoBox(String wikiText) {
			final String INFOBOX_OPEN_STR = "{{Infobox";
			int startPos = wikiText.indexOf(INFOBOX_OPEN_STR);
			if (startPos < 0)
				return null;
			int closeBracketCount = 2;
			int endPos = startPos + INFOBOX_OPEN_STR.length();
			for (; endPos < wikiText.length(); endPos++) {
				switch (wikiText.charAt(endPos)) {
				case '}':
					closeBracketCount--;
					break;
				case '{':
					closeBracketCount++;
					break;
				default:
				}
				if (closeBracketCount == 0)
					break;
			}
			String infoBoxText = wikiText.substring(startPos, endPos + 1);
			return infoBoxText;
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Pattern pat = Pattern.compile("\\t");

		public void map(Text k, Text v, OutputCollector<Text, Text> output,
				Reporter repoter) throws IOException {

		}

		@Override
		public void reduce(Text k, Iterator<Text> v,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (v.hasNext()) {
				String value = v.next().toString();
				sb.append(value);
				sb.append('\t');
			}
			output.collect(k, new Text(sb.toString()));

		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InfoboxGroup.class);
		conf.setJobName("Infobox Group");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
