package com.github.diaosi.BDAM.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class TestXMLParser {

	/**
	 * @param args
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 */
	public static void main(String[] args) throws ParserConfigurationException,
			SAXException, IOException {
		BufferedReader fin = new BufferedReader(new InputStreamReader(
				new FileInputStream("/data/Metadata_Phase1_Task4/2")));
		StringBuffer sb = new StringBuffer();
		String line = null;
		while ((line = fin.readLine()) != null) {
			sb.append(line + '\n');
		}
		InputSource is = new InputSource(new StringReader(sb.toString()));
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(is);
		doc.getDocumentElement().normalize();

		NodeList nList = doc.getElementsByTagName("page");
		String title = null;
		Element nNode = (Element) nList.item(0);
		NodeList childList = nNode.getChildNodes();
		for (int i = 0; i < childList.getLength(); i++) {
			Node n = childList.item(i);
			if (n.getNodeName().equals("title")) {
				title = n.getTextContent();
			}
			if (n.getNodeName().equals("revision")) {
				NodeList cl = n.getChildNodes();
				for (int j = 0; j < cl.getLength(); j++) {
					Node e = cl.item(j);
					if (e.getNodeName().equals("crosslanguage")) {
						String lang = e.getTextContent().trim();
						
					}
				}
			}
		}

	}
}
