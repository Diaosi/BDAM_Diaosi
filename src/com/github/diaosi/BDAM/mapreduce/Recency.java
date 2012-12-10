package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.xml.sax.SAXException;

import com.github.diaosi.BDAM.inputformat.XmlInputFormat;
import com.github.diaosi.BDAM.mapreduce.InfoboxGetter;
import com.github.diaosi.BDAM.mapreduce.InfoboxGetter.Map;
import com.github.diaosi.BDAM.outputformat.CsvOutputFormat;

public class Recency {
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, IntWritable> {
		
		private DocumentBuilderFactory dbFactory;
		private DocumentBuilder dBuilder;
		
		public Map() throws Exception {
			super();
			dbFactory = DocumentBuilderFactory.newInstance();
			dBuilder = dbFactory.newDocumentBuilder();
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			InputSource is = new InputSource(new StringReader(
					value.toString()));
			Document doc;
			try {
				doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("revision");
				
				Node nNode = nList.item(0);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					
					String recencyDate = parseRecencyDate(getTagValue("timestamp", eElement));
					if (recencyDate.length() == 10) output.collect(new Text(recencyDate), new IntWritable(1));
				}
			} catch (SAXException e) {
				e.printStackTrace();
			}
			
		}
		
		private static String getTagValue(String sTag, Element eElement) {
			NodeList nList = eElement.getElementsByTagName(sTag).item(0)
					.getChildNodes();
			Node nValue = nList.item(0);
			return nValue.getNodeValue();
		}
		
		private static String parseRecencyDate(String datetime) {
			String[] datetimeSplited = datetime.split("[T]");
			return datetimeSplited[0];
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Recency.class);
		conf.setJobName("Recency");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
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
