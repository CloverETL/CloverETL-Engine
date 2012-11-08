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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jetel.graph.runtime.EngineInitializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


/**
 * Class implementing export of data record metadata to XSD.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 12/14/06  
 */
public class XsdMetadata extends MXAbstract {

	// XSD document
	private Document doc;

	/**
	 * XSLT transformer for output serialization
	 */
	private Transformer outputTransformer;
	
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
		Element recordElement = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_ELEMENT);
		recordElement.setAttribute(NAME, metadata.getName());
		recordElement.setAttribute(TYPE, metadata.getName() + "Type");
		rootElement.appendChild(recordElement);

		Element typeElement = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_COMPLEX_TYPE);
		typeElement.setAttribute(NAME, metadata.getName() + "Type");
		rootElement.appendChild(typeElement);
		
		Element seqElement = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_SEQUENCE);
		typeElement.appendChild(seqElement);
		for (int idx = 0; idx < fields.length; idx++) {
			String typeName = getXsdType(doc, fields[idx]);
			Element fieldElement = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_ELEMENT);
			fieldElement.setAttribute(NAME, fields[idx].getName());
			fieldElement.setAttribute(TYPE, typeName);
			fieldElement.setAttribute(MIN_OCCURS, fields[idx].isNullable() ? "0" : "1");
			fieldElement.setAttribute(MAX_OCCURS, "1");
			seqElement.appendChild(fieldElement);
		}
		
		outputTransformer = createTransformer();
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
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(filename));
		write(outputStream);
		outputStream.close();
	}

	public void write(OutputStream output) throws IOException {
		try {
			outputTransformer.transform(new DOMSource(doc), new StreamResult(output));
		} catch (TransformerException e) {
			throw new IOException(e);
		}
	}

	private Transformer createTransformer() {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer;
		try {
			transformer = tf.newTransformer();
		} catch (TransformerConfigurationException e) {
			throw new RuntimeException("Unexpected error. Output XSLT transformer cannot be created.");
		}
    	transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    	transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
    	
    	return transformer;
	}
	
	/**
	 * Create "empty" XSD document
	 * @return
	 * @throws ParserConfigurationException
	 */
	private static Document createXsdDocument() throws ParserConfigurationException {
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		Element preamble = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_SCHEMA);
		preamble.setAttribute(XMLNS_XSD, XMLSCHEMA);
		if (NAMESPACE != null) {
			//preamble.setAttribute("targetNamespace", NAMESPACE);
			preamble.setAttribute(XMLNS, NAMESPACE);
			preamble.setAttribute("elementFormDefault", "qualified");
		}
		doc.appendChild(preamble);
		return doc;
	}

	/**
	 * Find specified XSD type in the XSD document.
	 * @param doc
	 * @param typeName
	 * @return
	 */
	private static Element getXsdType(Document doc, String nameSpace, String typeName) {
		String[] searchTags = new String[]{XSD_SIMPLE_TYPE, XSD_COMPLEX_TYPE};
		for (int tagIdx = 0; tagIdx < searchTags.length; tagIdx++) {
			NodeList xsdTypes = doc.getDocumentElement().getElementsByTagName(nameSpace + NAMESPACE_DELIMITER + searchTags[tagIdx]);
			for (int idx = 0; idx < xsdTypes.getLength(); idx++) {
				Element xsdType = (Element)xsdTypes.item(idx);
				if (typeName.equals(xsdType.getAttribute(NAME))) {
					return xsdType;
				}
			}
		}
		return null;
	}

	/**
	 * Returns name of XSD type which represents specified field. In case such a type doesn't exists,
	 * it's created.
	 * @param doc
	 * @param field
	 * @return XSD type
	 */
	private static String getXsdType(Document doc, DataFieldMetadata field) {
		String fieldTypeName = getFieldTypeName(field);
		boolean found = false;
		for (String nameSpace: NAMESPACES) {
			if (getXsdType(doc, nameSpace, fieldTypeName) != null) {
				found = true;
			}
		}
		if (!found) createXsdType(doc, field, fieldTypeName);
		return fieldTypeName;
	}

	/**
	 * Gets a field type name.
	 * @param field
	 * @return
	 */
	private static String getFieldTypeName(DataFieldMetadata field) {
		String basicName = typeNames.get(Character.valueOf(field.getType()));
		
		switch (field.getType()) {
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			if (field.getSize() > 0) basicName += "Size" + field.getSize();
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			basicName += "Length" + field.getProperty(LENGTH) + "Scale" + field.getProperty(SCALE);
			break;
		case DataFieldMetadata.STRING_FIELD:
			if (field.getSize() > 0) basicName += "Size" + field.getSize();
			if (field.getFormatStr() != null) basicName += "Format" + field.getFormatStr().hashCode();
			break;
		default:
			break;
		}
		return basicName;
	}

	/**
	 * Create XSD type representing specified field. The resulting type is derived from field type's base type and
	 * it may have additional restrictions. In case additional restrictions are not the necessary, the base type itself
	 * is returned. In case appropriately restricted type already exists, the method doesn't create new type
	 * and returns the existing one.
	 */
	private static void createXsdType(Document doc, DataFieldMetadata field, String fieldTypeName) {
		// basic properties
		Element typeElement = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_SIMPLE_TYPE);
		doc.getDocumentElement().appendChild(typeElement);
		typeElement.setAttribute(NAME, fieldTypeName);

		Element restr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_RESTRICTION);
		typeElement.appendChild(restr);
		restr.setAttribute(BASE, primitiveNames.get(Character.valueOf(field.getType())));
		
		// restrictions
		Element typeRestr;
		switch (field.getType()) {
		case DataFieldMetadata.BOOLEAN_FIELD:
			typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_PATTERN);
			typeRestr.setAttribute(VALUE, "true|false");
			restr.appendChild(typeRestr);
			break;
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			if (field.getSize() > 0) {
				typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_LENGHT);
				typeRestr.setAttribute(VALUE, Integer.toString(field.getSize()));
				restr.appendChild(typeRestr);
			}
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_TOTAL_DIGITS);
			typeRestr.setAttribute(VALUE, field.getProperty(LENGTH));
			restr.appendChild(typeRestr);
			typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_FRACTION_DIGITS);
			typeRestr.setAttribute(VALUE, field.getProperty(SCALE));
			restr.appendChild(typeRestr);
			break;
		case DataFieldMetadata.STRING_FIELD:
			if (field.getSize() > 0) {
				typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_LENGHT);
				typeRestr.setAttribute(VALUE, Integer.toString(field.getSize()));
				restr.appendChild(typeRestr);
			}
			if (field.getFormatStr() != null) {
				typeRestr = doc.createElement(NAMESPACES[0] + NAMESPACE_DELIMITER + XSD_PATTERN);
				typeRestr.setAttribute(VALUE, field.getFormatStr());
				restr.appendChild(typeRestr);
			}
			break;
		default:
			break;
		}
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
