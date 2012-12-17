package com.github.diaosi.BDAM.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class InfoboxGroup {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private String mid = "{{Infobox ";

		public void map(LongWritable k, Text v,
				OutputCollector<Text, Text> output, Reporter repoter)
				throws IOException {
			String line = v.toString();
			int pos = line.indexOf(mid);
			if (pos != -1) {
				int before = line.substring(0, pos).lastIndexOf(',');
				if (before <= 0)
					return;
				String id = line.substring(0, before);
				if (id.charAt(0) == '"')
					id = id.substring(1);
				if (id.endsWith("\""))
					id = id.substring(0, id.length() - 1);
				if (id.length() <= 0)
					return;
				pos = pos + mid.length();
				String infobox = line.substring(pos).trim();
				if (infobox.endsWith("\"")) {
					infobox = infobox.substring(0, infobox.length() - 1);
				}
				int endPos = infobox.indexOf("|");
				if (endPos != -1)
					infobox = infobox.substring(0, endPos);
				endPos = infobox.indexOf("}");
				if (endPos != -1)
					infobox = infobox.substring(0, endPos);
				if (infobox.length() > 0)
					output.collect(new Text(infobox), new Text(id));

			}

		}

	}

	public static class Combine extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text k, Iterator<Text> v,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (v.hasNext()) {
				String value = v.next().toString();
				sb.append(value);
				sb.append('|');
			}
			output.collect(k, new Text(sb.toString()));

		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Pattern p = Pattern.compile("\\|");

		@Override
		public void reduce(Text k, Iterator<Text> v,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int cnt = 0;
			StringBuilder sb = new StringBuilder("\t");
			while (v.hasNext()) {
				String value = v.next().toString();
				String[] words = p.split(value);
				for (String s : words)
					if (s.length() > 0) {
						sb.append(s);
						sb.append("|");
						cnt++;
					}
			}
			sb.insert(0, cnt);
			output.collect(k, new Text(sb.toString()));

		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InfoboxGroup.class);
		conf.setJobName("Infobox Group");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
