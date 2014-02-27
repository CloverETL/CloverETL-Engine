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
package org.jetel.metadata;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jetel.exception.XMLConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Transforms xsd document into DataRecordMetadata.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin (www.opensys.eu)
 */
public class MetadataXsd extends MXAbstract {

	private Document doc;
	private String recordDelimiter;
	private String fieldDelimiter;
	private short defaultFieldSize = -1;
	
	/**
	 * Constructor.
	 * 
	 * @param doc xsd document
	 */
	public MetadataXsd(Document doc, String recordDelimiter, String fieldDelimiter) {
		this.doc = doc;
		this.recordDelimiter = recordDelimiter;
		this.fieldDelimiter = fieldDelimiter;
	}
	public MetadataXsd(Document doc, short defaultFieldSize) {
		this.doc = doc;
		this.defaultFieldSize = defaultFieldSize;
	}
	
	/**
	 * Creates DataRecordMetadata from a xsd document.
	 * 
	 * @return
	 * @throws Exception
	 */
	public DataRecordMetadata createDataRecordMetadata() throws Exception {
		Node rootElement = doc.getDocumentElement();
		Node recordNode = getNode(rootElement, NAMESPACES, XSD_ELEMENT);
		if (recordNode == null) throw new Exception("Element '[xs:|xsd:]" + XSD_ELEMENT + "' not found.");
		String recordName = getAttributeValue(recordNode, NAME);
		if (recordName == null) throw new Exception("Record name attribute not found.");
		
		DataRecordMetadata metadata;
		if (fieldDelimiter != null) {
			metadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordMetadata.DELIMITED_RECORD);
			metadata.setRecordDelimiter(recordDelimiter);
		}
		else if (defaultFieldSize > -1) metadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordMetadata.FIXEDLEN_RECORD);
		else metadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME);
		
		metadata.setLabel(recordName);
		
		createFields(metadata, rootElement);
		
		metadata.normalize();
		return metadata;
	}

	/**
	 * Creates fields from xsd.
	 * 
	 * @param metadata
	 * @param node
	 */
	private void createFields(DataRecordMetadata metadata, Node node) {
		Node complexNode = getNode(node, NAMESPACES, XSD_COMPLEX_TYPE);
		if (complexNode == null) return;
		
		Node seqFields = getNode(complexNode, NAMESPACES, XSD_SEQUENCE);
		if (seqFields == null) return;
		NodeList list = seqFields.getChildNodes();
		
		Node field;
		String name;
		String type;
		String minOccurs;
		for (int i=0; i<list.getLength(); i++) {
			field = list.item(i);
			if (!equalsName(field.getNodeName(), XSD_ELEMENT)) continue;
			name = getAttributeValue(field, NAME);
			if (name == null) continue;
			type = getAttributeValue(field, TYPE);
			if (type == null) continue;
			minOccurs = getAttributeValue(field, MIN_OCCURS);

			// creates field metadata
			DataFieldMetadata dataFieldMetadata;
			switch (metadata.getRecType()) {
			case DataRecordMetadata.FIXEDLEN_RECORD:
				dataFieldMetadata = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, defaultFieldSize);
				break;
			default:
				dataFieldMetadata = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, fieldDelimiter);
				break;
			}
			dataFieldMetadata.setLabel(name);
			
			dataFieldMetadata.setNullable(minOccurs != null && minOccurs.equals("0") ? true : false);
			setField(dataFieldMetadata, findFieldTypeNode(node, type), type);
			metadata.addField(dataFieldMetadata);
		}
	}
	
	/**
	 * Finds the simply type node for a nameType.
	 * 
	 * @param node
	 * @param nameType
	 * @return
	 */
	private Node findFieldTypeNode(Node node, String nameType) {
		if (node == null) return null;
		NodeList list = node.getChildNodes();
		Node child;
		String nameTypeCur;	
		for (int i=0; i<list.getLength(); i++) {
			child = list.item(i);
			if (!equalsName(child.getNodeName(), XSD_SIMPLE_TYPE)) continue;
			if ((nameTypeCur=getAttributeValue(child, NAME)) != null && nameTypeCur.equals(nameType)) return child;
		}
		return null;
	}
	
	/**
	 * If names for namespaces are the same, the method returns true.
	 * @param name1
	 * @param name2
	 * @return
	 */
	private boolean equalsName(String name1, String name2) {
		boolean found = false;
		for (String nameSpace: NAMESPACES) {
			if (name1.equals(nameSpace + NAMESPACE_DELIMITER + name2)) {
				found = true; 
				break;
			}
		}
		return found;
	}
	
	/**
	 * Sets field attributes.
	 * 
	 * @param dataFieldMetadata
	 * @param node
	 * @param sType
	 */
	private void setField(DataFieldMetadata dataFieldMetadata, Node node, String sType) {
		Node restParent;
		restParent = getNode(node, NAMESPACES, XSD_RESTRICTION);
		String sValue = getAttributeValue(restParent, BASE);
		
		Character type = namesPrimitive.get(sValue != null ? sValue : sType);
		if (type == null) throw new RuntimeException("Unknown primitive data type '" + (sValue != null ? sValue : sType));
		if (type == DataFieldMetadata.BYTE_FIELD && sType.contains(CLOVER_BYTE_COMPRESSED)) 
			type = Character.valueOf(DataFieldMetadata.BYTE_FIELD_COMPRESSED);
		
		Node rest;
		switch (type) {
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			if ((rest = getNode(restParent, NAMESPACES, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Integer.parseInt(getAttributeValue(rest, VALUE)));
			}
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			if ((rest = getNode(restParent, NAMESPACES, XSD_TOTAL_DIGITS)) == null) {
				type = DataFieldMetadata.NUMERIC_FIELD;
			} else {
				dataFieldMetadata.setProperty(LENGTH, getAttributeValue(rest, VALUE));
			}
			if ((rest = getNode(restParent, NAMESPACES, XSD_FRACTION_DIGITS)) != null) {
				dataFieldMetadata.setProperty(SCALE, getAttributeValue(rest, VALUE));
			}
			if ((rest = getNode(restParent, NAMESPACES, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Integer.parseInt(getAttributeValue(rest, VALUE)));
			}
			break;
		case DataFieldMetadata.STRING_FIELD:
			if ((rest = getNode(restParent, NAMESPACES, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Integer.parseInt(getAttributeValue(rest, VALUE)));
			}
			if ((rest = getNode(restParent, NAMESPACES, XSD_PATTERN)) != null) {
				dataFieldMetadata.setFormatStr(getAttributeValue(rest, VALUE));
			}
			break;
		default:
			break;
		}
		dataFieldMetadata.setType(type);
	}

	/**
	 * Gets attribute value.
	 * 
	 * @param node
	 * @param attributeName
	 * @return
	 */
	private String getAttributeValue(Node node, String attributeName) {
		if (node == null) return null;
		NamedNodeMap namedNodeMap = node.getAttributes();
		Node attr;
		if ((attr = namedNodeMap.getNamedItem(attributeName)) == null) return null;
		return attr.getNodeValue();
	}
	
	/**
	 * Gets particular node.
	 * 
	 * @param node
	 * @param nodeName
	 * @return
	 */
	private Node getNode(Node node, String nameSpaces[], String nodeName) {
		if (node == null) return null;
		if (equalsName(node.getNodeName(), nodeName)) return node;
		NodeList list = node.getChildNodes();
		if (list == null) return null;
		Node tmpNode;
		for (int i=0; i<list.getLength(); i++) {
			tmpNode = getNode(list.item(i), nameSpaces, nodeName);
			if (tmpNode != null) return tmpNode;
		}
		return null;
	}
	
	public static void main(String argv[]) {
		
		Options options = new Options();
    	options.addOption(new Option("i", "in_file", true, "Input file"));
    	options.addOption(new Option("o", "out_file", true, "Output file"));
    	options.addOption(new Option("s", "field_size", true, "Default field size"));
    	options.addOption(new Option("r", "record_delimiter", true, "Record delimiter"));
    	options.addOption(new Option("f", "field_delimiter", true, "Field delimiter"));
    	options.addOption(new Option("h", "help", true, "Help"));
    	PosixParser optParser = new PosixParser();
    	CommandLine cmdLine;
		try {
			cmdLine = optParser.parse(options, argv);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		try {
			InputStream input;
        	OutputStream output;
        	String recordDelimiter = null;
        	String fieldDelimiter = null;
        	short defaultFieldSize = 0;
			if (cmdLine.hasOption("h")) {
		        System.out.println("MetadataXsd -i in_file -o out_file -s field_size");
		        System.out.println("MetadataXsd -i in_file -o out_file -r record_delimiter -f field_delimiter");
			}
			if (cmdLine.hasOption("i")) {
				input = new FileInputStream(cmdLine.getOptionValue("i"));
			} else {
				input = System.in;
			}
    		if (cmdLine.hasOption("o")) {
        		output = new FileOutputStream(cmdLine.getOptionValue("o"));
    		} else {
    			output = System.out;
    		}
    		if (cmdLine.hasOption("s")) {
    			defaultFieldSize = Short.parseShort(cmdLine.getOptionValue("s"));
    		}
    		if (cmdLine.hasOption("r")) {
    			recordDelimiter = cmdLine.getOptionValue("r");
    		}
    		if (cmdLine.hasOption("f")) {
    			fieldDelimiter = cmdLine.getOptionValue("f");
    		}
	        InputSource is = new InputSource(input);
	        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	        Document doc;
	        try {
	            doc = dbf.newDocumentBuilder().parse(is);
	        } catch (Exception e) {
	            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
	        }
	        
	        DataRecordMetadata metadata = null;
	        if (recordDelimiter != null && fieldDelimiter != null) 
	        	metadata = new MetadataXsd(doc, recordDelimiter, fieldDelimiter).createDataRecordMetadata();
	        else if (defaultFieldSize > 0)  
	        	metadata = new MetadataXsd(doc, defaultFieldSize).createDataRecordMetadata();
	        else {
	        	throw new Exception("The default field size or field/record delimeters is not specified.");
	        }
	        DataRecordMetadataXMLReaderWriter.write(metadata, output);
	        System.out.println("Metadata file created.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
