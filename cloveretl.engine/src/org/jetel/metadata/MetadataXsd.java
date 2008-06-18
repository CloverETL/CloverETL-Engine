package org.jetel.metadata;

import java.io.FileInputStream;
import java.io.InputStream;

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
	private String defaultDelimiter;
	private short defaultFieldSize = -1;
	
	/**
	 * Constructor.
	 * 
	 * @param doc xsd document
	 */
	public MetadataXsd(Document doc, String defaultDelimiter) {
		this.doc = doc;
		this.defaultDelimiter = defaultDelimiter;
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
		Node recordNode = getNode(rootElement, XSD_ELEMENT);
		if (recordNode == null) throw new Exception("Element '" + XSD_ELEMENT + "' not found.");
		String recordName = getAttributeValue(recordNode, NAME);
		if (recordName == null) throw new Exception("Record name attribute not found.");
		
		DataRecordMetadata metadata;
		if (defaultDelimiter != null) metadata = new DataRecordMetadata(recordName, DataRecordMetadata.DELIMITED_RECORD);
		else if (defaultFieldSize > -1) metadata = new DataRecordMetadata(recordName, DataRecordMetadata.FIXEDLEN_RECORD);
		else metadata = new DataRecordMetadata(recordName);
		
		createFields(metadata, rootElement);
		return metadata;
	}

	/**
	 * Creates fields from xsd.
	 * 
	 * @param metadata
	 * @param node
	 */
	private void createFields(DataRecordMetadata metadata, Node node) {
		Node complexNode = getNode(node, XSD_COMPLEX_TYPE);
		if (complexNode == null) return;
		
		Node seqFields = getNode(complexNode, XSD_SEQUENCE);
		if (seqFields == null) return;
		NodeList list = seqFields.getChildNodes();
		
		Node field;
		String name;
		String type;
		for (int i=0; i<list.getLength(); i++) {
			field = list.item(i);
			if (!field.getNodeName().equals(XSD_ELEMENT)) continue;
			name = getAttributeValue(field, NAME);
			if (name == null) continue;
			type = getAttributeValue(field, TYPE);
			if (type == null) continue;
			
			// creates field metadata
			DataFieldMetadata dataFieldMetadata;
			switch (metadata.getRecType()) {
			case DataRecordMetadata.FIXEDLEN_RECORD:
				dataFieldMetadata = new DataFieldMetadata(name, defaultFieldSize);
				break;
			default:
				dataFieldMetadata = new DataFieldMetadata(name, defaultDelimiter);
				break;
			}
			
			setField(dataFieldMetadata, findFieldTypeNode(node, type));
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
			if (!child.getNodeName().equals(XSD_SIMPLE_TYPE)) continue;
			if ((nameTypeCur=getAttributeValue(child, NAME)) != null && nameTypeCur.equals(nameType)) return child;
		}
		return null;
	}
	
	/**
	 * Sets field attributes.
	 * 
	 * @param dataFieldMetadata
	 * @param node
	 */
	private void setField(DataFieldMetadata dataFieldMetadata, Node node) {
		Node restParent;
		if ((restParent = getNode(node, XSD_RESTRICTION)) == null) return;
		String sValue;
		if ((sValue = getAttributeValue(restParent, BASE)) == null) return;
		char type = namesPrimitive.get(sValue);
		
		Node rest;
		switch (type) {
		case DataFieldMetadata.BYTE_FIELD:
			if ((rest = getNode(restParent, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Short.parseShort(getAttributeValue(rest, VALUE)));
			}
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			if ((rest = getNode(restParent, XSD_TOTAL_DIGITS)) == null) {
				type = DataFieldMetadata.NUMERIC_FIELD;
			} else {
				dataFieldMetadata.setFieldProperty(LENGTH, getAttributeValue(rest, VALUE));
			}
			if ((rest = getNode(restParent, XSD_FRACTION_DIGITS)) != null) {
				dataFieldMetadata.setFieldProperty(SCALE, getAttributeValue(rest, VALUE));
			}
			if ((rest = getNode(restParent, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Short.parseShort(getAttributeValue(rest, VALUE)));
			}
			break;
		case DataFieldMetadata.STRING_FIELD:
			if ((rest = getNode(restParent, XSD_LENGHT)) != null) {
				dataFieldMetadata.setSize(Short.parseShort(getAttributeValue(rest, VALUE)));
			}
			if ((rest = getNode(restParent, XSD_PATTERN)) != null) {
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
	private Node getNode(Node node, String nodeName) {
		if (node == null) return null;
		if (node.getNodeName().equals(nodeName)) return node;
		NodeList list = node.getChildNodes();
		if (list == null) return null;
		Node tmpNode;
		for (int i=0; i<list.getLength(); i++) {
			tmpNode = getNode(list.item(i), nodeName);
			if (tmpNode != null) return tmpNode;
		}
		return null;
	}
	
	public static void main(String argv[]) {
		Options options = new Options();
    	options.addOption(new Option("i", "in_file", true, "Input file"));
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
			if (cmdLine.hasOption("i")) {
				input = new FileInputStream(cmdLine.getOptionValue("i"));
			} else {
				input = System.in;
			}
	        InputSource is = new InputSource(input);
	        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	        Document doc;
	        try {
	            doc = dbf.newDocumentBuilder().parse(is);
	        } catch (Exception e) {
	            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
	        }
	        DataRecordMetadata metadata = new MetadataXsd(doc, (short)10).createDataRecordMetadata();
	        System.out.println(metadata);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
