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
import com.github.diaosi.BDAM.*;

public class InfoboxGetter {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException{

            String xmlString = value.toString();

            //output.collect(key, value);
            //SAXBuilder builder = new SAXBuilder();
            //Reader in = new StringReader(xmlString);
            try {

                InputSource is = new InputSource(new StringReader(value.toString()));
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(is);
                doc.getDocumentElement().normalize();

                NodeList nList = doc.getElementsByTagName("page");

                Node nNode = nList.item(0);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String infoBoxText = parseInfoBox(getTagValue("text", eElement));
                    int id = Integer.parseInt(getTagValue("id", eElement));
                    if (infoBoxText != null) {
                        //context.write(new IntWritable(id), new Text(infoBoxText));
                        output.collect(new LongWritable(id), new Text(infoBoxText));
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

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);

        conf.setInputFormat(XmlInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        FileInputFormat.setInputPaths(conf, new Path("/user/hduser"));
        FileOutputFormat.setOutputPath(conf, new Path("/user/output"));

        JobClient.runJob(conf);
    }
}
