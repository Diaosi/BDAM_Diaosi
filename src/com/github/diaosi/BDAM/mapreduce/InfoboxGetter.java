package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;

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
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.csvreader.CsvWriter;
import com.github.diaosi.BDAM.inputformat.XmlInputFormat;
import com.github.diaosi.BDAM.outputformat.CsvOutputFormat;

public class InfoboxGetter {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			// String xmlString = value.toString();

			// output.collect(key, value);
			// SAXBuilder builder = new SAXBuilder();
			// Reader in = new StringReader(xmlString);
			try {

				InputSource is = new InputSource(new StringReader(
						value.toString()));
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
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
						// context.write(new IntWritable(id), new
						// Text(infoBoxText));
						StringWriter sw = new StringWriter();
						CsvWriter writer = new CsvWriter(sw, ',');
						writer.writeRecord(new String[] { id, infoBoxText });
						writer.flush();
						String res = sw.toString();
						output.collect(new LongWritable(0), new Text(res));
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

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InfoboxGetter.class);
		conf.setJobName("InfoboxGetter");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(CsvOutputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
