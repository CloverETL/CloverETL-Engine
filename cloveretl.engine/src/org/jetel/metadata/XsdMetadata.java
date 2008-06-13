/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.metadata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.util.primitive.TypedProperties;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

/**
 * Class implementing export of data record metadata to XSD.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 12/14/06  
 */
public class XsdMetadata extends MXAbstract {

	// XSD document
	private Document doc;

	/**
	 * Sole ctor. Creates XSD document representing specified data record metadata
	 * @param metadata
	 * @throws ParserConfigurationException
	 */
	public XsdMetadata(DataRecordMetadata metadata) throws ParserConfigurationException {
		doc = createXsdDocument();
		if (metadata == null) {
			return;
		}
		DataFieldMetadata[] fields = metadata.getFields();
		Element rootElement = doc.getDocumentElement();
		Element recordElement = doc.createElement(XSD_ELEMENT);
		String properties;
		recordElement.setAttribute(NAME, metadata.getName());
		recordElement.setAttribute(TYPE, metadata.getName() + "Type");
		if (metadata.getFieldDelimiterStr() != null) 
			recordElement.setAttribute(FIELD_DELIMITER, metadata.getFieldDelimiterStr());
		if (metadata.getFieldNamesHeader() != null) 
			recordElement.setAttribute(FIELD_NAMES_HEADER, metadata.getFieldNamesHeader());
		if (metadata.getLocaleStr() != null) 
			recordElement.setAttribute(LOCALE_STR, metadata.getLocaleStr());
		if (metadata.getRecordDelimiterStr() != null) 
			recordElement.setAttribute(RECORD_DELIMITER, metadata.getRecordDelimiterStr());
		recordElement.setAttribute(RECORD_SIZE, Integer.toString(metadata.getRecordSize()));
		recordElement.setAttribute(RECORD_SIZE_STRIP_AUTO_FILLING, Integer.toString(metadata.getRecordSizeStripAutoFilling()));
		recordElement.setAttribute(REC_TYPE, String.valueOf(metadata.getRecType()));
		if ((properties = getProperties(metadata.getRecordProperties())) != null) 
			recordElement.setAttribute(RECORD_PROPERTIES, properties);
		recordElement.setAttribute(SKIP_FIRST_LINE, Boolean.toString(metadata.isSkipFirstLine()));
		recordElement.setAttribute(NULLABLE, Boolean.toString(metadata.isNullable()));
		recordElement.setAttribute(SPECIFIED_FIELD_DELIMITER, Boolean.toString(metadata.isSpecifiedFieldDelimiter()));
		recordElement.setAttribute(SPECIFIED_RECORD_DELIMITER, Boolean.toString(metadata.isSpecifiedRecordDelimiter()));
		
		rootElement.appendChild(recordElement);

		Element typeElement = doc.createElement(XSD_COMPLEX_TYPE);
		typeElement.setAttribute(NAME, metadata.getName() + "Type");
		rootElement.appendChild(typeElement);
		
		Element seqElement = doc.createElement(XSD_SEQUENCE);
		typeElement.appendChild(seqElement);
		for (int idx = 0; idx < fields.length; idx++) {
			String typeName = getXsdType(doc, fields[idx]);
			Element fieldElement = doc.createElement(XSD_ELEMENT);
			fieldElement.setAttribute(NAME, fields[idx].getName());
			fieldElement.setAttribute(TYPE, typeName);
			if (fields[idx].getAutoFilling() != null) 
				fieldElement.setAttribute(AUTO_FILLING, fields[idx].getAutoFilling());
			if (fields[idx].getDefaultValueStr() != null) 
				fieldElement.setAttribute(DEFAULT_VALUE_STR, fields[idx].getDefaultValueStr());
			if (fields[idx].getDelimiterStr() != null) 
				fieldElement.setAttribute(DELIMITER_STR, fields[idx].getDelimiterStr());
			if (fields[idx].getFormatStr() != null) 
				fieldElement.setAttribute(FORMAT_STR, fields[idx].getFormatStr());
			if (fields[idx].getLocaleStr() != null) 
				fieldElement.setAttribute(LOCALE_STR, fields[idx].getLocaleStr());
			if ((properties = getProperties(fields[idx].getFieldProperties())) != null) 
				fieldElement.setAttribute(FIELD_PROPERTIES, properties);
			fieldElement.setAttribute(SHIFT, Short.toString(fields[idx].getShift()));
			fieldElement.setAttribute(SIZE, Short.toString(fields[idx].getSize()));
			seqElement.appendChild(fieldElement);
		}
	}

	private String getProperties(TypedProperties typedProperties) {
		StringBuilder sb = new StringBuilder();
        for(Enumeration<?> e = typedProperties.propertyNames(); e.hasMoreElements();) {
            String propertyName = (String) e.nextElement();
            sb.append(propertyName).append(PROPERTIES_ASSIGN).append(typedProperties.getProperty(propertyName)).append(PROPERTIES_DELIM);
        }
		return sb.toString().equals("") ? null : sb.toString();
	}
	
	/**
	 * 
	 * @return XSD representation of metadata
	 */
	public Document getXsd() {
		return doc;
	}

	/**
	 * Write XSD to specified file
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void write(String filename) throws FileNotFoundException, IOException {
		write(new BufferedOutputStream(new FileOutputStream(new File(filename))));
	}

	public void write(OutputStream output) throws IOException {
		OutputFormat fmt = new OutputFormat(doc);
		fmt.setIndenting(true);
		new XMLSerializer(output, fmt).serialize(doc);				
	}

	/**
	 * Create "empty" XSD document
	 * @return
	 * @throws ParserConfigurationException
	 */
	private static Document createXsdDocument() throws ParserConfigurationException {
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		Element preamble = doc.createElement("xsd:schema");
		preamble.setAttribute("xmlns:xsd", XMLSCHEMA);
		if (NAMESPACE != null) {
			preamble.setAttribute("targetNamespace", NAMESPACE);
			preamble.setAttribute("xmlns", NAMESPACE);
			preamble.setAttribute("elementFormDefault", "qualified");
		}
		doc.appendChild(preamble);
		return doc;
	}

	/**
	 * Create base XSD type for specified field type
	 * @param doc
	 * @param type
	 * @return
	 */
	private static Element createBaseType(Document doc, char type) {
		Element typeElement = doc.createElement("xsd:simpleType");
		doc.getDocumentElement().appendChild(typeElement);
		typeElement.setAttribute("name", "" + typeNames.get(Character.valueOf(type)));

		Element restr = doc.createElement("xsd:restriction");
		typeElement.appendChild(restr);
		restr.setAttribute("base", primitiveNames.get(Character.valueOf(type)));
		
		switch (type) {
		case DataFieldMetadata.NUMERIC_FIELD:
			Element fr = doc.createElement("xsd:fractionDigits");
			fr.setAttribute("value", "0");
			restr.appendChild(fr);
			break;
		default:
		}

		return typeElement;
	}

	/**
	 * Find specified XSD type in the XSD document.
	 * @param doc
	 * @param typeName
	 * @return
	 */
	private static Element getXsdType(Document doc, String typeName) {
		String[] searchTags = new String[]{"xsd:simpleType", "xsd:complexType"};
		for (int tagIdx = 0; tagIdx < searchTags.length; tagIdx++) {
			NodeList xsdTypes = doc.getDocumentElement().getElementsByTagName(searchTags[tagIdx]);
			for (int idx = 0; idx < xsdTypes.getLength(); idx++) {
				Element xsdType = (Element)xsdTypes.item(idx);
				if (typeName.equals(xsdType.getAttribute("name"))) {
					return xsdType;
				}
			}
		}
		return null;
	}

	/**
	 * Create XSD type representing specified field. The resulting type is derived from field type's base type and
	 * it may have additional restrictions. In case additional restrictions are not the necessary, the base type itself
	 * is returned. In case appropriately restricted type already exists, the method doesn't create new type
	 * and returns the existing one.
	 * @param doc
	 * @param field
	 * @param baseType
	 * @return
	 */
	private static Element createRestrictedType(Document doc, DataFieldMetadata field, Element baseType) {
		// TODO more restrictions
		Element sizeRestr = null;
		String subname = "" + baseType.getAttribute("name");

		// create various restrictions according to field metadata
		if (field.getSize() > 0) {
			subname += "Size" + field.getSize();
			sizeRestr = doc.createElement("xsd:length"); // TODO special code for BYTE_FIELD
			sizeRestr.setAttribute("value", String.valueOf(field.getSize()));
		}

		if (sizeRestr == null) {	// no additional restrictions
			return baseType;
		}
		// try to obtain pre-existing restricted subtype 
		Element subtype = getXsdType(doc, subname);
		if (subtype != null) {	// return pre-existing restricted subtype
			return subtype;
		}
		// create restricted subtype
		Element restriction = doc.createElement("xsd:restriction");
		restriction.appendChild(sizeRestr);
		subtype = doc.createElement("xsd:simpleType");
		subtype.setAttribute("name", subname);
		subtype.appendChild(restriction);
		doc.getDocumentElement().appendChild(subtype);
		return subtype;
	}

	/**
	 * Returns name of XSD type which represents specified field. In case such a type doesn't exists,
	 * it's created.
	 * @param doc
	 * @param field
	 * @return XSD type
	 */
	private static String getXsdType(Document doc, DataFieldMetadata field) {
		Element baseType = getXsdType(doc, typeNames.get(Character.valueOf(field.getType())));
		if (baseType == null) {
			baseType = createBaseType(doc, field.getType());
		}
		return createRestrictedType(doc, field, baseType).getAttribute(NAME);
	}

	/**
	 * Converts clover metadata read from XML file to XSD written to file.
	 * XML file can be metadata file or graph file. If XML file is graph, 
	 * then you define metadata_id.
	 * 
	 * usage: XsdMetadata [--metadata_id id] [infile] [outfile]
	 * @param argv {input_file, output_file}. "-" for std input/output
	 */
	public static void main(String argv[]) {
		String metadataId = null;
		
        try {
        	InputStream input;
        	OutputStream output;
       	
    		Options options = new Options();
        	options.addOption(new Option("m", "metadata_id", true, "Methadata id in graph file"));
        	options.addOption(new Option("i", "in_file", true, "Input file"));
        	options.addOption(new Option("o", "out_file", true, "Output file"));
        	
        	PosixParser optParser = new PosixParser();
        	CommandLine cmdLine;
    		try {
    			cmdLine = optParser.parse(options, argv);
    		} catch (ParseException e) {
    			e.printStackTrace();
    			return;
    		}
    		
    		if (cmdLine.hasOption("m")) {
    			metadataId = cmdLine.getOptionValue("m");
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

    	    EngineInitializer.initEngine((String) null, null, null);
    		
            DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
            DataRecordMetadata metadata;
            if (metadataId == null)
            	metadata = xmlReader.read(new BufferedInputStream(input));
            else 
            	metadata = xmlReader.read(new BufferedInputStream(input), metadataId);
            
            if(metadata == null) {
                System.err.println("Metadata doesn't exist." + (metadataId != null ? " (" + metadataId + ")" : ""));
                return;
            }
			(new XsdMetadata(metadata)).write(output);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
	}

}
