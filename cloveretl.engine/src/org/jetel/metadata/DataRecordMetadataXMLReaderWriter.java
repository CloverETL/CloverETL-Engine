/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
// FILE: c:/projects/jetel/org/jetel/metadata/DataRecordMetadataReaderWriter.java
package org.jetel.metadata;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.PropertyRefResolver;
import org.jetel.util.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Helper class for reading/writing DataRecordMetadata (record structure)
 * from/to XML format <br>
 * 
 * <i>Note: If for record or some field Node are defined more attributes than
 * those recognized, they are transferred to Java Properties object and assigned
 * to either record or field. </i>
 * 
 * The XML DTD describing the internal structure is as follows:
 * 
 * <pre>
 * 
 *   &lt;!ELEMENT Record (FIELD+)&gt;
 *   &lt;!ATTLIST Record
 * 		name ID #REQUIRED
 *             	type ( fixed | delimited )  #REQUIRED &gt;
 * 
 * 
 *   &lt;!ELEMENT Field (#PCDATA) EMPTY&gt;
 *   &lt;!ATTLIST Field
 *       name ID #REQUIRED
 * 		type NMTOKEN #REQUIRED
 * 		delimiter NMTOKEN #IMPLIED &quot;,&quot;
 * 		size NMTOKEN #IMPLIED &quot;0&quot;
 * 		format CDATA #IMPLIED 
 * 		locale CDATA #IMPLIED
 *       nullable NMTOKEN #IMPLIED &quot;true&quot;
 * 		default CDATA #IMPLIED &gt;
 *   
 * </pre>
 * 
 * Example:
 * 
 * <pre>
 * 
 *    &lt;Record name=&quot;TestRecord&quot; type=&quot;delimited&quot;&gt;
 * 	&lt;Field name=&quot;Field1&quot; type=&quot;numeric&quot; delimiter=&quot;;&quot; /&gt;
 * 	&lt;Field name=&quot;Field2&quot; type=&quot;numeric&quot; delimiter=&quot;;&quot; locale=&quot;de&quot; /&gt;
 * 	&lt;Field name=&quot;Field3&quot; type=&quot;string&quot; delimiter=&quot;;&quot; /&gt;
 * 	&lt;Field name=&quot;Field4&quot; type=&quot;string&quot; delimiter=&quot;\n&quot;/&gt;
 *    &lt;/Record&gt;
 *   
 * </pre>
 * 
 * If you specify your own attributes, they will be accessible through
 * getFieldProperties() method of DataFieldMetadata class. <br>
 * Example:
 * 
 * <pre>
 * 
 *   &lt;Field name=&quot;Field1&quot; type=&quot;numeric&quot; delimiter=&quot;;&quot; mySpec1=&quot;1&quot; mySpec2=&quot;xyz&quot;/&gt;
 *   
 * </pre>
 * 
 * @author D.Pavlis
 * @since May 6, 2002
 * @see javax.xml.parsers
 */
public class DataRecordMetadataXMLReaderWriter extends DefaultHandler {

	// Attributes
	private DataRecordMetadata recordMetadata;

	private DocumentBuilder db;

	private DocumentBuilderFactory dbf;

	private PropertyRefResolver refResolver;

	// not needed private final static String DEFAULT_PARSER_NAME = "";
	private static final boolean validation = false;

	private static final boolean ignoreComments = true;

	private static final boolean ignoreWhitespaces = true;

	private static final boolean putCDATAIntoText = false;

	private static final boolean createEntityRefs = false;

	//private static boolean setLoadExternalDTD = true;

	//private static boolean setSchemaSupport = true;
	//private static boolean setSchemaFullSupport = false;
	private static final String RECORD_ELEMENT = "Record";
	private static final String FIELD_ELEMENT = "Field";
	private static final String CODE_ELEMENT = "Code";
	private static final String NAME_ATTR = "name"; 
	private static final String TYPE_ATTR = "type";
	private static final String DELIMITER_ATTR = "delimiter";
	private static final String FORMAT_ATTR = "format";
	private static final String DEFAULT_ATTR = "default";
	private static final String LOCALE_ATTR = "locale";
	private static final String NULLABLE_ATTR = "nullable";
	private static final String SIZE_ATTR = "size";
	
	private static final String DEFAULT_CHARACTER_ENCODING = "UTF-8";

	private static Log logger = LogFactory.getLog(DataRecordMetadata.class);

	// Associations

	// Operations
	/**
	 * An operation that reads DataRecord format definition from XML data
	 * 
	 * @param in
	 *            InputStream with XML data describing DataRecord
	 * @return DataRecordMetadata object
	 * @since May 6, 2002
	 */
	public DataRecordMetadata read(InputStream in) {
		Document document;

		try {
			// construct XML parser (if it is needed)
			if ((dbf == null) || (db == null)) {
				dbf = DocumentBuilderFactory.newInstance();

				dbf.setNamespaceAware(true);

				// Optional: set various configuration options
				dbf.setValidating(validation);
				dbf.setIgnoringComments(ignoreComments);
				dbf.setIgnoringElementContentWhitespace(ignoreWhitespaces);
				dbf.setCoalescing(putCDATAIntoText);
				dbf.setExpandEntityReferences(!createEntityRefs);

				db = dbf.newDocumentBuilder();
			}

			document = db.parse(new BufferedInputStream(in));
			document.normalize();

		} catch (SAXParseException ex) {
			logger.fatal(ex.getMessage() + " --> on line "
					+ ex.getLineNumber() + " row " + ex.getColumnNumber());
			return null;
		} catch (ParserConfigurationException ex) {
			logger.fatal(ex.getMessage());
			return null;
		} catch (Exception ex) {
			logger.fatal(ex.getMessage());
			return null;
		}

		try {
			return parseRecordMetadata(document);
		} catch (DOMException ex) {
			logger.fatal(ex.getMessage());
			return null;
		} catch (Exception ex) {
			logger.fatal("parseRecordMetadata method call: "
					+ ex.getMessage());
			return null;
		}
	}

	/**
	 * An operation that writes DataRecord format definition into XML format
	 * 
	 * @param record
	 *            Metadata describing data record
	 * @param outStream
	 *            OutputStream into which XML data is written
	 * @since May 6, 2002
	 */
	public static void write(DataRecordMetadata record, OutputStream outStream) {
        DocumentBuilder db = null;
	    try {
	        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	        db = dbf.newDocumentBuilder();
		    Document doc = db.newDocument();
		    Element rootElement = doc.createElement(RECORD_ELEMENT);
		    doc.appendChild(rootElement);
		    write(record, rootElement);
        
		    TransformerFactory tf = TransformerFactory.newInstance();
		    Transformer t = tf.newTransformer();
	        t.transform(new DOMSource(doc), new StreamResult(outStream));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
	}
	
	public static void write(DataRecordMetadata record, Element metadataElement) {
		DataFieldMetadata field;

		metadataElement.setAttribute(NAME_ATTR, record.getName());
		metadataElement.setAttribute(TYPE_ATTR,
		        record.getRecType() == DataRecordMetadata.DELIMITED_RECORD
		        		? "delimited" : "fixed");

		Properties prop = record.getRecordProperties();
		if (prop != null) {
			Enumeration enumeration = prop.propertyNames();
			while (enumeration.hasMoreElements()) {
				String key = (String) enumeration.nextElement();
				metadataElement.setAttribute(key, prop.getProperty(key));
			}
		}

		// OUTPUT FIELDS
		
//		MessageFormat fieldForm = new MessageFormat(
//				"\t<Field name=\"{0}\" type=\"{1}\" ");

		char[] fieldTypeLimits = { DataFieldMetadata.STRING_FIELD,
				DataFieldMetadata.DATE_FIELD, 
				DataFieldMetadata.DATETIME_FIELD,
				DataFieldMetadata.NUMERIC_FIELD,
				DataFieldMetadata.INTEGER_FIELD,
				DataFieldMetadata.LONG_FIELD,
				DataFieldMetadata.DECIMAL_FIELD, 
				DataFieldMetadata.BYTE_FIELD };
		String[] fieldTypeParts = { "string", "date", "datetime", "numeric",
				"integer", "long", "decimal", "byte" };

		Document doc = metadataElement.getOwnerDocument();
		for (int i = 0; i < record.getNumFields(); i++) {
			field = record.getField(i);			
			
			if (field != null) {

			    Element fieldElement = doc.createElement(FIELD_ELEMENT);
				metadataElement.appendChild(fieldElement);

				fieldElement.setAttribute(NAME_ATTR, field.getName());
			    fieldElement.setAttribute(TYPE_ATTR,
			            fieldTypeFormat(field.getType(), fieldTypeLimits,
						fieldTypeParts));

				if (record.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
					fieldElement.setAttribute(DELIMITER_ATTR,
					        StringUtils.specCharToString(field.getDelimiter()));
				} else {
					fieldElement.setAttribute(SIZE_ATTR, String.valueOf(field.getSize()));
				}
				if (field.getFormatStr() != null) {
					fieldElement.setAttribute(FORMAT_ATTR, field.getFormatStr());
				}
				if (field.getDefaultValue() != null) {
					fieldElement.setAttribute(DEFAULT_ATTR, field.getDefaultValue());
				}
				if (field.getLocaleStr() != null) {
					fieldElement.setAttribute(LOCALE_ATTR, field.getLocaleStr());
				}
				fieldElement.setAttribute(NULLABLE_ATTR,
				        String.valueOf(field.isNullable()));

				// output field properties - if anything defined
				prop = field.getFieldProperties();
				if (prop != null) {
					Enumeration enumeration = prop.propertyNames();
					while (enumeration.hasMoreElements()) {
						String key = (String) enumeration.nextElement();
						fieldElement.setAttribute(key, prop.getProperty(key));
					}
				}

				if (field.getCodeStr() != null) {
				    Element codeElement = doc.createElement(CODE_ELEMENT);
				    fieldElement.appendChild(codeElement);
				    Text codeText = doc.createTextNode(field.getCodeStr());
				    codeElement.appendChild(codeText);
				}

			}
		}
	}

	public DataRecordMetadata parseRecordMetadata(Document document)
			throws DOMException {
		org.w3c.dom.NodeList nodes;
		nodes = document.getElementsByTagName(RECORD_ELEMENT);
		if (nodes.getLength() == 0) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"No Record element has been found ! ");
		}

		return parseRecordMetadata(nodes.item(0));
	}

	public DataRecordMetadata parseRecordMetadata(org.w3c.dom.Node topNode)
			throws DOMException {

		org.w3c.dom.NamedNodeMap attributes;
		DataRecordMetadata recordMetadata;
		String recordName = null;
		String recordType = null;
		String recLocaleStr = null;
		String itemName;
		String itemValue;
		Properties recordProperties = null;
		PropertyRefResolver refResolver = new PropertyRefResolver();

		if (topNode.getNodeName() != RECORD_ELEMENT) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"Root Node is not of type " + RECORD_ELEMENT);
		}

		/*
		 * get Record level metadata
		 */
		attributes = topNode.getAttributes();
		for (int i = 0; i < attributes.getLength(); i++) {
			itemName = attributes.item(i).getNodeName();
			itemValue = refResolver.resolveRef(attributes.item(i)
					.getNodeValue());
			if (itemName.equalsIgnoreCase("name")) {
				recordName = itemValue;
			} else if (itemName.equalsIgnoreCase("type")) {
				recordType = itemValue;
			} else if (itemName.equalsIgnoreCase("locale")) {
				recLocaleStr = itemValue;
			} else {
				if (recordProperties == null) {
					recordProperties = new Properties();
				}
				recordProperties.setProperty(itemName, itemValue);
			}
		}

		if (recordType == null || recordName == null) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"Attribute \"name\" or \"type\" not defined within Record !");
		}

		recordMetadata = new DataRecordMetadata(recordName, recordType
				.equalsIgnoreCase("fixed") ? DataRecordMetadata.FIXEDLEN_RECORD
				: DataRecordMetadata.DELIMITED_RECORD);
		if (recLocaleStr != null) {
			recordMetadata.setLocaleStr(recLocaleStr);
		}
		recordMetadata.setRecordProperties(recordProperties);

		/*
		 * parse metadata of FIELDs
		 */

		NodeList fieldElements = topNode.getChildNodes();
		for (int i = 0; i < fieldElements.getLength(); i++) {
			if (!fieldElements.item(i).getNodeName().equals(FIELD_ELEMENT)) {
				continue;
			}
			attributes = fieldElements.item(i).getAttributes();
			DataFieldMetadata field;
			String format = null;
			String defaultValue = null;
			String name = null;
			String size = null;
			String delimiter = null;
			String nullable = null;
			String localeStr = null;
			char fieldType = ' ';
			Properties fieldProperties = null;

			for (int j = 0; j < attributes.getLength(); j++) {
				itemName = attributes.item(j).getNodeName();
				itemValue = refResolver.resolveRef(attributes.item(j)
						.getNodeValue());
				if (itemName.equalsIgnoreCase("type")) {
					fieldType = getFieldType(itemValue);
				} else if (itemName.equalsIgnoreCase("name")) {
					name = itemValue;
				} else if (itemName.equalsIgnoreCase("size")) {
					size = itemValue;
				} else if (itemName.equalsIgnoreCase("delimiter")) {
					delimiter = itemValue;
				} else if (itemName.equalsIgnoreCase("format")) {
					format = itemValue;
				} else if (itemName.equalsIgnoreCase("default")) {
					defaultValue = itemValue;
				} else if (itemName.equalsIgnoreCase("nullable")) {
					nullable = itemValue;
				} else if (itemName.equalsIgnoreCase("locale")) {
					localeStr = itemValue;
				} else {
					if (fieldProperties == null) {
						fieldProperties = new Properties();
					}
					fieldProperties.setProperty(itemName, itemValue);
				}
			}

			if ((fieldType == ' ') || (name == null)) {
				throw new DOMException(DOMException.NOT_FOUND_ERR,
						"Attribute \"name\" or \"type\" not defined for field #"
								+ i);
			}

			// create FixLength field or Delimited base on Record Type
			if (recordMetadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
				if (size == null) {
					throw new DOMException(DOMException.NOT_FOUND_ERR,
							"Attribute \"size\" not defined for field #" + i);
				}
				field = new DataFieldMetadata(name, fieldType,
						getFieldSize(size));
			} else {
				if (delimiter == null) {
					throw new DOMException(DOMException.NOT_FOUND_ERR,
							"Attribute \"delimiter\" not defined for field #"
									+ i);
				}
				field = new DataFieldMetadata(name, fieldType, StringUtils
						.stringToSpecChar(delimiter));
			}
			// set properties
			field.setFieldProperties(fieldProperties);

			// set format string if defined
			if (format != null) {
				field.setFormatStr(format);
			}
			if (defaultValue != null) {
				field.setDefaultValue(defaultValue);
			}

			// set nullable if defined
			if (nullable != null) {
				field.setNullable(nullable.matches("^[tTyY].*"));
			}
			// set localeStr if defined
			if (localeStr != null) {
				field.setLocaleStr(localeStr);
			}

			NodeList fieldCodeElements = fieldElements.item(i).getChildNodes();
			if (fieldCodeElements != null) {
				for (int l = 0; l < fieldCodeElements.getLength(); l++) {
					if (fieldCodeElements.item(l).getNodeName().equals(
							CODE_ELEMENT)) {
						field.setCodeStr(fieldCodeElements.item(l)
								.getFirstChild().getNodeValue());
					}
				}
			}
			recordMetadata.addField(field);
		}
		// check that at least one valid field definition has been found
		if (recordMetadata.getNumFields() == 0) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"No Field elements have been found ! ");
		}

		return recordMetadata;
	}

	/**
	 * Gets the FieldType attribute of the DataRecordMetadataXMLReaderWriter
	 * object
	 * 
	 * @param fieldType
	 *            Description of Parameter
	 * @return The FieldType value
	 * @since May 6, 2002
	 */
	private char getFieldType(String fieldType) {

		if (fieldType.equalsIgnoreCase("string")) {
			return DataFieldMetadata.STRING_FIELD;
		}
		if (fieldType.equalsIgnoreCase("date")) {
			return DataFieldMetadata.DATE_FIELD;
		}
		if (fieldType.equalsIgnoreCase("datetime")) {
			return DataFieldMetadata.DATETIME_FIELD;
		}
		if (fieldType.equalsIgnoreCase("numeric")) {
			return DataFieldMetadata.NUMERIC_FIELD;
		}
		if (fieldType.equalsIgnoreCase("integer")) {
			return DataFieldMetadata.INTEGER_FIELD;
		}
		if (fieldType.equalsIgnoreCase("long")) {
			return DataFieldMetadata.LONG_FIELD;
		}
		if (fieldType.equalsIgnoreCase("decimal")) {
			return DataFieldMetadata.DECIMAL_FIELD;
		}
		if (fieldType.equalsIgnoreCase("byte")) {
			return DataFieldMetadata.BYTE_FIELD;
		}

		throw new RuntimeException("Unrecognized field type specified!");
	}

	/**
	 * Gets the FieldSize attribute of the DataRecordMetadataXMLReaderWriter
	 * object
	 * 
	 * @param fieldSizeStr
	 *            Description of Parameter
	 * @return The FieldSize value
	 * @exception SAXException
	 *                Description of Exception
	 * @since May 6, 2002
	 */
	private short getFieldSize(String fieldSizeStr) {
		return Short.parseShort(fieldSizeStr);
	}

	/**
	 * Description of the Method
	 * 
	 * @param fieldType
	 *            Description of Parameter
	 * @param fieldTypeOptions
	 *            Description of Parameter
	 * @param fieldTypeStrings
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @since May 6, 2002
	 */
	private final static String fieldTypeFormat(char fieldType,
			char[] fieldTypeOptions, String[] fieldTypeStrings) {
		for (int i = 0; i < fieldTypeOptions.length; i++) {
			if (fieldTypeOptions[i] == fieldType) {
				return fieldTypeStrings[i];
			}
		}
		return "";
	}

}
/*
 *  end class DataRecordMetadataXMLReaderWriter
 */

