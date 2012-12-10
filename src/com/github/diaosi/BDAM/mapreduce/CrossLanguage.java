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

import com.github.diaosi.BDAM.inputformat.XmlInputFormat;

public class CrossLanguage {
	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private DocumentBuilderFactory dbFactory;
		private DocumentBuilder dBuilder;

		public MyMapper() throws Exception {
			super();
			dbFactory = DocumentBuilderFactory.newInstance();
			dBuilder = dbFactory.newDocumentBuilder();
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			try {

				InputSource is = new InputSource(new StringReader(
						value.toString()));
				Document doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("page");
				Element nNode = (Element) nList.item(0);
				NodeList childList = nNode.getChildNodes();
				for (int i = 0; i < childList.getLength(); i++) {
					Node n = childList.item(i);
					if ("revision".equals(n.getNodeName())) {
						NodeList cl = n.getChildNodes();
						for (int j = 0; j < cl.getLength(); j++) {
							Node e = cl.item(j);
							if ("crosslanguage".equals(e.getNodeName())) {
								String lang = e.getTextContent().trim()
										.toLowerCase();
								output.collect(new Text(lang), new IntWritable(
										1));
								// System.err.format("%s:%d\n", lang, 1);
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, LongWritable> {

		@Override
		public void reduce(Text k, Iterator<IntWritable> vs,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long count = 0;
			while (vs.hasNext()) {
				IntWritable i = vs.next();
				count += i.get();
			}
			output.collect(k, new LongWritable(count));
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CrossLanguage.class);
		conf.setJobName("CrossLanguage");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
