/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.jetel.data.parser.AbstractSpreadsheetParser.SpreadsheetMappingMode;
import org.jetel.data.parser.AbstractSpreadsheetParser.SpreadsheetOrientation;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18 Aug 2011
 */
public class XLSMapping {
	
	public static final String DEFAULT_VERSION = "1.0";
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	public static final int DEFAULT_SKIP = 0;
	public static final SpreadsheetMappingMode DEFAULT_AUTO_MAPPING_MODE = SpreadsheetMappingMode.AUTO;
	
	public static final int UNDEFINED = -1;
	
	private static final String XPATH_GLOBAL_ATTRIBUTES = "/mapping/globalAttributes/";
	private static final String XPATH_STEP_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "step/text()";
	private static final String XPATH_ORIENTATION_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "orientation/text()";
	//private static final String XPATH_WRITE_HEADER_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "writeHeader/text()"; //TODO: add for writer

	private static final String XML_HEADER_GROUP = "headerGroup";
	private static final String XML_HEADER_GROUP_CLOVER_FIELD = "cloverField";
	private static final String XML_HEADER_GROUP_SKIP = "skip";
	private static final String XML_HEADER_GROUP_AUTO_MAPPING_TYPE = "autoMappingType";
	private static final String XML_HEADER_RANGES = "headerRanges";
	private static final String XML_HEADER_RANGE_BEGIN = "begin";
	private static final String XML_HEADER_RANGE_END = "end";

	private static final Pattern HEADER_PATTERN = Pattern.compile("([A-Z]{1,3})([1-9][0-9]{0,6})");
	
	private static final XLSMapping DEFAULT_MAPPING;
	static {
		List<HeaderGroup> groups = Collections.emptyList();
		DEFAULT_MAPPING = new XLSMapping(1, SpreadsheetOrientation.HORIZONTAL, groups);
	}
	
	private final int step;
	private final SpreadsheetOrientation orientation;
	
	private final List<HeaderGroup> headerGroups;
	
	private XLSMapping(int step, SpreadsheetOrientation orientation, List<HeaderGroup> groups) {
		this.step = step;
		this.orientation = orientation;
		this.headerGroups = groups;
	}
	
	public int getStep() {
		return step;
	}

	public SpreadsheetOrientation getOrientation() {
		return orientation;
	}

	public List<HeaderGroup> getHeaderGroups() {
		return headerGroups;
	}
	
	public static XLSMapping getDefault() {
		return DEFAULT_MAPPING;
	}
	
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		//TODO: implement this!
		return status;
	}
	
	public static XLSMapping parse(String inputString, DataRecordMetadata metadata) throws ComponentNotReadyException {
		InputSource is = new InputSource(new StringReader(inputString));
		
		try {
			return parse(getBuilder().parse(is), metadata);
		} catch (SAXException e) {
			 throw new ComponentNotReadyException(e);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	public static XLSMapping parse(InputStream inputStream, DataRecordMetadata metadata) throws ComponentNotReadyException {
		
		try {
			return parse(getBuilder().parse(inputStream), metadata);
		} catch (SAXException e) {
			 throw new ComponentNotReadyException(e);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	private static DocumentBuilder getBuilder() {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema");
			factory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", "XLSMapping.xsd");

			return factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	private static XLSMapping parse(Document document, DataRecordMetadata metadata) {
		try {
			XPathFactory xpathFactory = XPathFactory.newInstance();
			XPath xpath = xpathFactory.newXPath();

			Double intResult = (Double) xpath.evaluate(XPATH_STEP_ATTRIBUTE, document, XPathConstants.NUMBER);
			String orientationResult = (String) xpath.evaluate(XPATH_ORIENTATION_ATTRIBUTE, document, XPathConstants.STRING);

			return new XLSMapping(intResult.intValue(), SpreadsheetOrientation.valueOfIgnoreCase(orientationResult),
					parseHeaderGroups(document.getElementsByTagName(XML_HEADER_GROUP), metadata));
		} catch (XPathExpressionException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	private static List<HeaderGroup> parseHeaderGroups(NodeList headerGroups, DataRecordMetadata metadata) {
		List<HeaderGroup> headerGroupList = new ArrayList<HeaderGroup>(headerGroups.getLength());
		
		Map<String, Integer> fieldMap = metadata.getFieldNamesMap();
		
		for (int i = 0; i < headerGroups.getLength(); i++) {
			Node group = headerGroups.item(i);
			NamedNodeMap attributes = group.getAttributes();
			
			int skip = DEFAULT_SKIP;
			Node skipAttribute = attributes.getNamedItem(XML_HEADER_GROUP_SKIP);
			if (skipAttribute != null) {
				skip = Integer.parseInt(skipAttribute.getNodeValue()); 
			}
			
			int cloverFieldIndex = UNDEFINED;
			SpreadsheetMappingMode mappingMode = DEFAULT_AUTO_MAPPING_MODE;
			List<HeaderRange> headerRanges = null;

			NodeList groupProperties = group.getChildNodes();
			for (int j = 0; j < groupProperties.getLength(); j++) {
				Node property = groupProperties.item(j);
				String propertyName = property.getNodeName(); 
			
				if (XML_HEADER_RANGES.equals(propertyName)) {
					headerRanges = parseHeaderRanges(property.getChildNodes());
				} else if (XML_HEADER_GROUP_CLOVER_FIELD.equals(propertyName) && property.getFirstChild() != null) {
					
					int isInteger = StringUtils.isInteger(property.getFirstChild().getNodeValue()); 
					if (isInteger == 0 || isInteger == 1) {
						cloverFieldIndex = Integer.parseInt(property.getFirstChild().getNodeValue());
					} else {
						Integer index = fieldMap.get(property.getFirstChild().getNodeValue());
						cloverFieldIndex = index != null ? index : UNDEFINED; //TODO: fail instead?
					}
				} else if (XML_HEADER_GROUP_AUTO_MAPPING_TYPE.equals(propertyName) && property.getFirstChild() != null) {
					mappingMode = SpreadsheetMappingMode.valueOfIgnoreCase(property.getFirstChild().getNodeValue());
				}
			}
			
			headerGroupList.add(new HeaderGroup(skip, cloverFieldIndex, mappingMode, headerRanges));
		}
		
		return headerGroupList;
	}
	
	private static List<HeaderRange> parseHeaderRanges(NodeList headerRanges) {
		List<HeaderRange> ranges = new ArrayList<HeaderRange>(headerRanges.getLength());
		
		int rowStart;
		int rowEnd;
		int columnStart;
		int columnEnd;
		
		for (int rangeCounter = 0; rangeCounter < headerRanges.getLength(); rangeCounter++) {
			Node range = headerRanges.item(rangeCounter);
			if (range.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			NamedNodeMap rangeAttributes = range.getAttributes();
			
			Matcher matcher = HEADER_PATTERN.matcher(rangeAttributes.getNamedItem(XML_HEADER_RANGE_BEGIN).getNodeValue());
			matcher.matches();
			
			columnStart = columnEnd = SpreadsheetUtils.getColumnIndex(matcher.group(1));
			rowStart = rowEnd = Integer.parseInt(matcher.group(2)) - 1;
			
			Node end = rangeAttributes.getNamedItem(XML_HEADER_RANGE_END);
			if (end != null) {
				matcher = HEADER_PATTERN.matcher(end.getNodeValue());
				matcher.matches();
				columnEnd = SpreadsheetUtils.getColumnIndex(matcher.group(1));
				rowEnd = Integer.parseInt(matcher.group(2)) - 1;
			}
			
			ranges.add(new HeaderRange(rowStart, rowEnd, columnStart, columnEnd));
		}
		
		return ranges;
	}
	
	public static class HeaderGroup {
		private final int skip;
		private final int cloverField;
		private final SpreadsheetMappingMode mappingMode;
		
		private final List<HeaderRange> ranges;
		
		public HeaderGroup(int skip, int cloverField, SpreadsheetMappingMode mappingMode, List<HeaderRange> ranges) {
			this.skip = skip;
			this.cloverField = cloverField;
			this.mappingMode = mappingMode;
			this.ranges = Collections.unmodifiableList(ranges);
		}

		public int getSkip() {
			return skip;
		}

		public int getCloverField() {
			return cloverField;
		}

		public SpreadsheetMappingMode getMappingMode() {
			return mappingMode;
		}

		public List<HeaderRange> getRanges() {
			return ranges;
		}
	}

	public static class HeaderRange {
		
		private final int rowStart;
		private final int rowEnd;
		private final int columnStart;
		private final int columnEnd;
		
		public HeaderRange(int rowStart, int rowEnd, int columnStart, int columnEnd) {
			this.rowStart = rowStart;
			this.rowEnd = rowEnd;
			this.columnStart = columnStart;
			this.columnEnd = columnEnd;
		}

		public int getRowStart() {
			return rowStart;
		}

		public int getRowEnd() {
			return rowEnd;
		}

		public int getColumnStart() {
			return columnStart;
		}

		public int getColumnEnd() {
			return columnEnd;
		}
	}	
}


