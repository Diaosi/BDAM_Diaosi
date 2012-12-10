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
import org.xml.sax.SAXParseException;

import com.github.diaosi.BDAM.inputformat.XmlInputFormat;
import com.github.diaosi.BDAM.mapreduce.CategoryPage.Map;
import com.github.diaosi.BDAM.mapreduce.CategoryPage.Reduce;

public class PageWithNCate {
	
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private DocumentBuilderFactory dbFactory;
		private DocumentBuilder dBuilder;

		public Map() throws Exception {
			super();
			dbFactory = DocumentBuilderFactory.newInstance();
			dBuilder = dbFactory.newDocumentBuilder();
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			InputSource is = new InputSource(new StringReader(value.toString()));
			Document doc;
			try {
				doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("revision");
				Element nNode = (Element) nList.item(0);

				NodeList cateList = nNode.getElementsByTagName("category");
				
				output.collect(new IntWritable(cateList.getLength()), new IntWritable(1));
				
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements
	Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageWithNCate.class);
		conf.setJobName("PageWithNCate");

		conf.setOutputKeyClass(IntWritable.class);
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
