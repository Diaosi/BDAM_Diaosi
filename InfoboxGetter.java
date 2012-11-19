package com.github.diaosi.BDAM;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import org.xml.sax.InputSource;

public class InfoboxGetter {
    public static class Map extends MapReduceBase implements Mapper<IntWritable, Text, IntWritable, Text> {

        public void map(IntWritable key, Text value, Context context, Reporter reporter) throws IOException, InterruptedException {

            try {
                InputSource is = new InputSource(new StringReader(value.toString()));
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(is);
                doc.getDocumentElement().normalize();

                NodeList nList = doc.getElementsByTagName("page");

                boolean debug = true;
                int count = 0;
                for (int temp = 0; temp < nList.getLength(); temp++) {
                    if (debug == true) {
                        if (count > 100) break;
                        else ++count;
                    }
                    Node nNode = nList.item(temp);
                    if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) nNode;
                        String infoBoxText = parseInfoBox(getTagValue("text", eElement));
                        int id = Integer.parseInt(getTagValue("id", eElement));
                        if (infoBoxText != null) {
                            context.write(new IntWritable(id), new Text(infoBoxText));
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static String getTagValue(String sTag, Element eElement) {
            NodeList nList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
            Node nValue = nList.item(0);
            return nValue.getNodeValue();
        }

        private static String parseInfoBox(String wikiText) {
            final String INFOBOX_OPEN_STR = "{{Infobox";
            int startPos = wikiText.indexOf(INFOBOX_OPEN_STR);
            if(startPos < 0) return null;
            int closeBracketCount = 2;
            int endPos = startPos + INFOBOX_OPEN_STR.length();
            for(; endPos < wikiText.length(); endPos++) {
                switch(wikiText.charAt(endPos)) {
                    case '}':
                        closeBracketCount--;
                        break;
                    case '{':
                        closeBracketCount++;
                        break;
                    default:
                }
                if(closeBracketCount == 0) break;
            }
            String infoBoxText = wikiText.substring(startPos, endPos+1);
            return infoBoxText;
        }
    }

    public static void main (String [] args) throws Exception
    {
        JobConf conf = new JobConf(InfoboxGetter.class);
        conf.setJobName("InfoboxGetter");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("/user/hduser/hw1/hurricane-center-addresses.csv"));
        FileOutputFormat.setOutputPath(conf, new Path("/user/hduser/hw1-b-output"));

        JobClient.runJob(conf);
    }
}
