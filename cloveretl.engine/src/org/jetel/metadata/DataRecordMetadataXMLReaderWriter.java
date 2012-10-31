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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecordNature;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.KeyFieldNamesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
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
 * 	            name ID #REQUIRED
 *              id ID #REQUIRED
 *              name CDATA #REQUIRED
 *              type NMTOKEN (delimited | fixed | mixed) #REQUIRED
 *              locale CDATA #IMPLIED
 *              recordDelimiter CDATA #IMPLIED
 * 
 * 
 *   &lt;!ELEMENT Field (#PCDATA) EMPTY&gt;
 *   &lt;!ATTLIST Field
 *              name ID #REQUIRED
 *              type NMTOKEN #REQUIRED
 *              delimiter NMTOKEN #IMPLIED &quot;,&quot;
 *              size NMTOKEN #IMPLIED &quot;0&quot;
 *              shift NMTOKEN #IMPLIED &quot;0&quot;
 *              format CDATA #IMPLIED 
 *              locale CDATA #IMPLIED
 *              nullable NMTOKEN (true | false) #IMPLIED &quot;true&quot;
 *              compressed NMTOKEN (true | false) #IMPLIED &quot;false&quot;
 *              default CDATA #IMPLIED &gt;
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
	private static final String METADATA_ELEMENT = "Metadata";
	private static final String ID = "id";
	private static final String RECORD_ELEMENT = "Record";
	private static final String FIELD_ELEMENT = "Field";
	//private static final String CODE_ELEMENT = "Code";
	private static final String NAME_ATTR = "name"; 
	private static final String LABEL_ATTR = "label"; 
    private static final String TYPE_ATTR = "type";
    private static final String CONTAINER_TYPE_ATTR = "containerType";
    private static final String RECORD_SIZE_ATTR = "recordSize";
    public  static final String RECORD_DELIMITER_ATTR = "recordDelimiter";
    public  static final String FIELD_DELIMITER_ATTR = "fieldDelimiter";
	private static final String DELIMITER_ATTR = "delimiter";
	public static final String EOF_AS_DELIMITER_ATTR = "eofAsDelimiter";
	private static final String FORMAT_ATTR = "format";
	private static final String DEFAULT_ATTR = "default";
	private static final String LOCALE_ATTR = "locale";
	private static final String NULLABLE_ATTR = "nullable";
	private static final String NULL_VALUE_ATTR = "nullValue";
	private static final String COMPRESSED_ATTR = "compressed";
	private static final String SHIFT_ATTR = "shift";
	private static final String SIZE_ATTR = "size";
	private static final String SKIP_SOURCE_ROW_ATTR = "skipSourceRows";
	private static final String QUOTED_STRINGS = "quotedStrings";
	private static final String QUOTE_CHAR = "quoteChar";
	private static final String KEY_FIELD_NAMES_ATTR = "keyFieldNames";
	private static final String AUTO_FILLING_ATTR = "auto_filling";
	public static final String CONNECTION_ATTR = "connection";
	private static final String COLLATOR_SENSITIVITY_ATTR = "collator_sensitivity";
	private static final String NATURE_ATTR = "nature";
	
	private static final String DEFAULT_CHARACTER_ENCODING = "UTF-8";

	  private static final String XSL_FORMATER
  	= "<?xml version='1.0' encoding='"+DEFAULT_CHARACTER_ENCODING+"'?>"
      + "<xsl:stylesheet version='1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>"
      + "<xsl:output encoding='"+DEFAULT_CHARACTER_ENCODING+"' indent='yes' method='xml'/>"
      + "<xsl:template match='/'><xsl:copy-of select='*'/></xsl:template>"
      + "</xsl:stylesheet>";
	
	private static Log logger = LogFactory.getLog(DataRecordMetadata.class);

    /**
     * Constructor with graph to property resolving.
     * @param graph
     */
    public DataRecordMetadataXMLReaderWriter() {
        this((Properties) null);
    }

    /**
     * Constructor with graph to property resolving.
     * @param graph
     */
    public DataRecordMetadataXMLReaderWriter(TransformationGraph graph) {
        this(graph.getGraphProperties());
    }
    
    /**
     * Constructor with properties for resolving.
     * @param properties
     */
    public DataRecordMetadataXMLReaderWriter(Properties properties) {
        refResolver = new PropertyRefResolver(properties);
    }
    
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
		return read(in, null);
	}
	
	public DataRecordMetadata read(InputStream in, String metadataId) {
		Document document;

		try {
			// construct XML parser (if it is needed)
			if ((dbf == null) || (db == null)) {
				dbf = DocumentBuilderFactory.newInstance();

				dbf.setNamespaceAware(true);
                dbf.setCoalescing(true);

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
			logger.error("SAX Exception on line "
					+ ex.getLineNumber() + " row " + ex.getColumnNumber(), ex);
			return null;
		} catch (ParserConfigurationException ex) {
			logger.error(ex.getMessage());
			return null;
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			return null;
		}

		try {
			return parseRecordMetadata(document, metadataId);
		} catch (DOMException ex) {
			logger.error(ex.getMessage());
			return null;
		} catch (Exception ex) {
			logger.error("parseRecordMetadata method call: "
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
        DocumentBuilder db;
	    try {
	        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	        db = dbf.newDocumentBuilder();
		    Document doc = db.newDocument();
		    Element rootElement = doc.createElement(RECORD_ELEMENT);
		    doc.appendChild(rootElement);
		    write(record, rootElement);
        
		    StreamSource formSrc = new StreamSource(new StringReader(XSL_FORMATER));
		    TransformerFactory tf = TransformerFactory.newInstance();
		    Transformer t = tf.newTransformer(formSrc);
	        t.transform(new DOMSource(doc), new StreamResult(outStream));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
	}
	
	public static void write(DataRecordMetadata record, Element metadataElement) {
		String rt;
		DataFieldMetadata field;

		metadataElement.setAttribute(NAME_ATTR, record.getName());
		
		String label = record.getLabel();
		if (!StringUtils.isEmpty(label)) {
			metadataElement.setAttribute(LABEL_ATTR, StringUtils.specCharToString(label));
		}

		if(record.getParsingType() == DataRecordParsingType.DELIMITED) rt = "delimited";
		else if(record.getParsingType() == DataRecordParsingType.FIXEDLEN) rt = "fixed";
		else rt = "mixed";

        metadataElement.setAttribute(TYPE_ATTR, rt);

        if(record.isSpecifiedRecordDelimiter()) {
            metadataElement.setAttribute(RECORD_DELIMITER_ATTR, StringUtils.specCharToString(record.getRecordDelimiter()));
        }

        if (record.getRecordSize() != 0) {
            metadataElement.setAttribute(RECORD_SIZE_ATTR, String.valueOf(record.getRecordSize()));
        }
        
        if (record.isSpecifiedFieldDelimiter()) {
        	metadataElement.setAttribute(FIELD_DELIMITER_ATTR, StringUtils.specCharToString(record.getFieldDelimiter()));
        }

        if (record.getSkipSourceRows() > 0) {
        	metadataElement.setAttribute(SKIP_SOURCE_ROW_ATTR, String.valueOf(record.getSkipSourceRows()));
        }
        
        if (record.isQuotedStrings()) {
        	metadataElement.setAttribute(QUOTED_STRINGS, String.valueOf(true));
        }
        
        if (record.getQuoteChar() != null) {
        	metadataElement.setAttribute(QUOTE_CHAR, String.valueOf(record.getQuoteChar()));
        }

        if (record.getKeyFieldNames() != null && !record.getKeyFieldNames().isEmpty()) {
        	metadataElement.setAttribute(KEY_FIELD_NAMES_ATTR, KeyFieldNamesUtils.getFieldNamesAsString(record.getKeyFieldNames()));
        }
        

		Properties prop = record.getRecordProperties();
		if (prop != null) {
			Enumeration<?> enumeration = prop.propertyNames();
			while (enumeration.hasMoreElements()) {
				String key = (String) enumeration.nextElement();
				metadataElement.setAttribute(key, prop.getProperty(key));
			}
		}

		
		Document doc = metadataElement.getOwnerDocument();
		
		// OUTPUT FIELDS
		
//		MessageFormat fieldForm = new MessageFormat(
//				"\t<Field name=\"{0}\" type=\"{1}\" ");
		for (int i = 0; i < record.getNumFields(); i++) {
			field = record.getField(i);			
			
			if (field != null) {

			    Element fieldElement = doc.createElement(FIELD_ELEMENT);
			    String delimiterStr = StringUtils.specCharToString(field.getDelimiter());
			    
				metadataElement.appendChild(fieldElement);

				fieldElement.setAttribute(NAME_ATTR, field.getName());
				label = field.getLabel();
				if (!StringUtils.isEmpty(label)) {
				    fieldElement.setAttribute(LABEL_ATTR, StringUtils.specCharToString(label));
				}
			    fieldElement.setAttribute(TYPE_ATTR, DataFieldMetadata.type2Str(field.getType()));
			    if (field.getContainerType() != null && field.getContainerType() != DataFieldContainerType.SINGLE) {
			    	fieldElement.setAttribute(CONTAINER_TYPE_ATTR, field.getContainerType().toString());
			    }

				fieldElement.setAttribute(SHIFT_ATTR, String.valueOf(field.getShift()));
				if (record.getParsingType() == DataRecordParsingType.DELIMITED 
						&& !StringUtils.isEmpty(delimiterStr)) {
					fieldElement.setAttribute(DELIMITER_ATTR, delimiterStr);
				} else {
					fieldElement.setAttribute(SIZE_ATTR, String.valueOf(field.getSize()));
				}
				if (field.getFormatStr() != null) {
					fieldElement.setAttribute(FORMAT_ATTR, field.getFormatStr());
				}
				if (field.getDefaultValueStr() != null) {
					fieldElement.setAttribute(DEFAULT_ATTR, field.getDefaultValueStr());
				}
				if (field.getLocaleStr() != null) {
					fieldElement.setAttribute(LOCALE_ATTR, field.getLocaleStr());
				}
				if (field.getAutoFilling() != null) {
					fieldElement.setAttribute(AUTO_FILLING_ATTR, field.getAutoFilling());
				}
				fieldElement.setAttribute(EOF_AS_DELIMITER_ATTR,
						String.valueOf(field.isEofAsDelimiter()));
				fieldElement.setAttribute(NULLABLE_ATTR,
				        String.valueOf(field.isNullable()));

				if (!StringUtils.isEmpty(field.getNullValue())) {
					fieldElement.setAttribute(NULL_VALUE_ATTR, field.getNullValue());
				}

				// output field properties - if anything defined
				prop = field.getFieldProperties();
				if (prop != null) {
					Enumeration<?> enumeration = prop.propertyNames();
					while (enumeration.hasMoreElements()) {
						String key = (String) enumeration.nextElement();
						fieldElement.setAttribute(key, prop.getProperty(key));
					}
				}

//				if (field.getCodeStr() != null) {
//				    Element codeElement = doc.createElement(CODE_ELEMENT);
//				    fieldElement.appendChild(codeElement);
//				    Text codeText = doc.createTextNode(field.getCodeStr());
//				    codeElement.appendChild(codeText);
//				}

			}
		}
	}

	public DataRecordMetadata parseRecordMetadata(Document document)
			throws DOMException {
		return parseRecordMetadata(document, null);
	}

	public DataRecordMetadata[] parseRecordMetadataAll(Document document) throws DOMException {
		org.w3c.dom.NodeList nodes = document.getElementsByTagName(RECORD_ELEMENT);
		int len = nodes.getLength();
		DataRecordMetadata[] allMetadata = new DataRecordMetadata[len];
		for (int i=0; i<len; i++) {
			allMetadata[i] = parseRecordMetadata(nodes.item(i));
		}
		return allMetadata;
	}

	public DataRecordMetadata parseRecordMetadata(Document document, String metadataId) throws DOMException {
		if (metadataId != null) {
			org.w3c.dom.NodeList nodes = document.getElementsByTagName(METADATA_ELEMENT);
			int lenght = nodes.getLength();
			org.w3c.dom.Node node = null;
			for (int i=0; i<lenght; i++) {
				node = nodes.item(i);
				if (node.getAttributes().getNamedItem(ID).getNodeValue().equals(metadataId)) {
					return parseRecordMetadata(node.getChildNodes().item(1));
				}
			}
			return null;
		}

		return parseRecordMetadata(document.getDocumentElement());
	}
	
	public DataRecordMetadata parseRecordMetadata(org.w3c.dom.Node topNode) throws DOMException {
		if (topNode == null || !RECORD_ELEMENT.equals(topNode.getNodeName())) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"Root node is not of type " + RECORD_ELEMENT);
		}

		org.w3c.dom.NamedNodeMap attributes;
		DataRecordMetadata recordMetadata;
		String recordName = null;
		String recordLabel = null;
		String recordType = null;
		String recordDelimiter = null;
		String fieldDelimiter = null;
		String sizeStr = null;
		String recLocaleStr = null;
		String recNullValue = null;
		String itemName;
		String itemValue;
		String skipSourceRows = null;
		String quotedStrings = null;
		String quoteChar = null;
		String collatorSensitivity = null;
		String nature = null;
		String keyFieldNamesStr = null;
		Properties recordProperties = null;

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
			} else if (itemName.equalsIgnoreCase(LABEL_ATTR)) {
				recordLabel = itemValue;
			} else if (itemName.equalsIgnoreCase(TYPE_ATTR)) {
				recordType = itemValue;
			} else if (itemName.equalsIgnoreCase("locale")) {
				recLocaleStr = itemValue;
			} else if (itemName.equalsIgnoreCase(NULL_VALUE_ATTR)) {
				recNullValue = itemValue;
			} else if (itemName.equalsIgnoreCase(RECORD_DELIMITER_ATTR)) {
				recordDelimiter = itemValue;
			} else if (itemName.equalsIgnoreCase(FIELD_DELIMITER_ATTR)) {
				fieldDelimiter = itemValue;
			} else if (itemName.equalsIgnoreCase(RECORD_SIZE_ATTR)) {
				sizeStr = itemValue;
			} else if (itemName.equalsIgnoreCase(QUOTED_STRINGS)) {
				quotedStrings = itemValue;
			} else if (itemName.equalsIgnoreCase(QUOTE_CHAR)) {
				quoteChar = itemValue;
			} else if (itemName.equalsIgnoreCase(KEY_FIELD_NAMES_ATTR)) {
				keyFieldNamesStr = itemValue;
			} else if (itemName.equalsIgnoreCase(SKIP_SOURCE_ROW_ATTR)) {
				skipSourceRows = itemValue;
			} else if (itemName.equalsIgnoreCase(COLLATOR_SENSITIVITY_ATTR)) {
				collatorSensitivity = itemValue;
			} else if (itemName.equalsIgnoreCase(NATURE_ATTR)) {
				nature = itemValue;
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

		DataRecordParsingType rt;
		if (recordType.equalsIgnoreCase("delimited")) rt = DataRecordParsingType.DELIMITED;
		else if (recordType.equalsIgnoreCase("fixed")) rt = DataRecordParsingType.FIXEDLEN;
		else if (recordType.equalsIgnoreCase("mixed")) rt = DataRecordParsingType.MIXED;
		else throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Unknown record type '" + recordType + "'. One of these options is supported delimited|fixed|mixed.");

		recordMetadata = new DataRecordMetadata(recordName, rt);
		if (!StringUtils.isEmpty(recordLabel)) {
			recordMetadata.setLabel(recordLabel);
		}
		if (recLocaleStr != null) {
			recordMetadata.setLocaleStr(recLocaleStr);
		}
		if (recNullValue != null) {
			recordMetadata.setNullValue(recNullValue);
		}
		recordMetadata.setRecordProperties(recordProperties);
		if(!StringUtils.isEmpty(recordDelimiter)) {
			recordMetadata.setRecordDelimiter(recordDelimiter);
		}
		if(!StringUtils.isEmpty(fieldDelimiter)) {
			recordMetadata.setFieldDelimiter(fieldDelimiter);
		}

		short recSize = 0;
		try {
			recSize = Short.parseShort(sizeStr);
		} catch (NumberFormatException e) {
			// ignore 
		}
		recordMetadata.setRecordSize(recSize);
		
		try {
			int iSkipSourceRows = -1;
			if (skipSourceRows != null)	iSkipSourceRows = Integer.parseInt(skipSourceRows);
			recordMetadata.setSkipSourceRows(iSkipSourceRows);
		} catch (NumberFormatException e) {
			// ignore 
		}
		
		if (!StringUtils.isEmpty(quoteChar)) {
			recordMetadata.setQuoteChar(QuotingDecoder.quoteCharFromString(quoteChar));
		}
		if (!StringUtils.isEmpty(quotedStrings)) {
			recordMetadata.setQuotedStrings(Boolean.valueOf(quotedStrings));
		}
		
		if (!StringUtils.isEmpty(keyFieldNamesStr)) {
			recordMetadata.setKeyFieldNames(KeyFieldNamesUtils.getFieldNamesAsList(keyFieldNamesStr));
		}

		if (!StringUtils.isEmpty(collatorSensitivity)) {
			recordMetadata.setCollatorSensitivity(collatorSensitivity);
		}
		
		if (!StringUtils.isEmpty(nature)) {
			recordMetadata.setNature(DataRecordNature.fromString(nature));
		}

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
			String label = null;
			String size = null;
			String shift = null;
			String delimiter = null;
			String eofAsDelimiter = null;
			String nullable = null;
			String nullValue = null;
			String localeStr = null;
			/*String*/ collatorSensitivity = null;
			String compressed = null;
			String autoFilling = null;
			String trim = null;
			char fieldType = ' ';
			DataFieldContainerType containerType = null;
			Properties fieldProperties = new Properties();

			for (int j = 0; j < attributes.getLength(); j++) {
				itemName = attributes.item(j).getNodeName();
                    itemValue = refResolver.resolveRef(attributes.item(j)
                            .getNodeValue());
				if (itemName.equalsIgnoreCase("type")) {
					fieldType = getFieldType(itemValue);
				} else if (itemName.equalsIgnoreCase(CONTAINER_TYPE_ATTR)) {
					containerType = DataFieldContainerType.fromString(itemValue);
				} else if (itemName.equalsIgnoreCase("name")) {
					name = itemValue;
				} else if (itemName.equalsIgnoreCase(LABEL_ATTR)) {
					label = itemValue;
				} else if (itemName.equalsIgnoreCase("size")) {
					size = itemValue;
				} else if (itemName.equalsIgnoreCase(SHIFT_ATTR)) {
					shift = itemValue;
				} else if (itemName.equalsIgnoreCase("delimiter")) {
					delimiter = itemValue;
				} else if (itemName.equalsIgnoreCase(EOF_AS_DELIMITER_ATTR)) {
					eofAsDelimiter = itemValue;
				} else if (itemName.equalsIgnoreCase("format")) {
					format = itemValue;
				} else if (itemName.equalsIgnoreCase("default")) {
					defaultValue = itemValue;
				} else if (itemName.equalsIgnoreCase("nullable")) {
					nullable = itemValue;
				} else if (itemName.equalsIgnoreCase(NULL_VALUE_ATTR)) {
					nullValue = itemValue;
				} else if (itemName.equalsIgnoreCase("locale")) {
					localeStr = itemValue;
				} else if (itemName.equalsIgnoreCase(COLLATOR_SENSITIVITY_ATTR)) {
					collatorSensitivity = itemValue;
				}else if (itemName.equalsIgnoreCase("trim")) {
					trim = itemValue;
				} else if (itemName.equalsIgnoreCase(COMPRESSED_ATTR)) {
					compressed = itemValue;
				} else if (itemName.equalsIgnoreCase(AUTO_FILLING_ATTR)) {
					autoFilling = itemValue;
				} else {
					fieldProperties.setProperty(itemName, itemValue);
				}
			}

			if (fieldType == DataFieldMetadata.BYTE_FIELD && Boolean.valueOf(compressed)) {
				fieldType = DataFieldMetadata.BYTE_FIELD_COMPRESSED;
			}

			if ((fieldType == ' ') || (name == null)) {
				throw new DOMException(DOMException.NOT_FOUND_ERR,
						"Attribute \"name\" or \"type\" not defined for field #"
								+ i);
			}

			short shiftVal = 0;
			try {
				shiftVal = Short.parseShort(shift);
			} catch (NumberFormatException e) {
				// ignore 
			}

			// create FixLength field or Delimited base on Record Type
			if (recordMetadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
				if (size == null) {
					throw new DOMException(DOMException.NOT_FOUND_ERR,
							"Attribute \"size\" not defined for field #" + i);
				}
				field = new DataFieldMetadata(name, fieldType,
						getFieldSize(size));
                field.setShift(shiftVal);
			} else if (recordMetadata.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
//				if (delimiter == null) {
//					throw new DOMException(DOMException.NOT_FOUND_ERR,
//							"Attribute \"delimiter\" not defined for field #"
//									+ i);
//				}
				field = new DataFieldMetadata(name, fieldType, delimiter);
                field.setShift(shiftVal);
			} else { //mixed dataRecord type
//				if (delimiter == null && size == null) {
//					throw new DOMException(DOMException.NOT_FOUND_ERR,
//							"Attribute \"delimiter\" either \"size\" not defined for field #"
//									+ i);
//				}
				if(delimiter != null) {
                    field = new DataFieldMetadata(name, fieldType, delimiter);
                    field.setShift(shiftVal);
                } else {
					field = new DataFieldMetadata(name, fieldType, getFieldSize(size));
                    field.setShift(shiftVal);
                }
			}
			
            field.setEofAsDelimiter( Boolean.valueOf( eofAsDelimiter ).booleanValue() );
			// set properties
			field.setFieldProperties(fieldProperties);

			// set container type
			if (containerType != null) {
				field.setContainerType(containerType);
			}
			// set the label
			if (label != null) {
				field.setLabel(label);
			}
			// set format string if defined
			if (format != null) {
				field.setFormatStr(format);
			}
			if (defaultValue != null) {
				field.setDefaultValueStr(defaultValue);
			}

			// set nullable if defined
			if (nullable != null) {
				field.setNullable(nullable.matches("^[tTyY].*"));
			}

			if (nullValue != null) {
				field.setNullValue(nullValue);
			}

			// set localeStr if defined
			if (localeStr != null) {
				field.setLocaleStr(localeStr);
			}

			// set collator sensitivity if defined
			if (collatorSensitivity != null) {
				field.setCollatorSensitivity(collatorSensitivity);
			}
			
			if (autoFilling != null) {
				field.setAutoFilling(autoFilling);
			}
			
			if (trim != null) {
				field.setTrim(trim.matches("^[tTyY].*"));
			}
			
			recordMetadata.addField(field);
		}
		
		return recordMetadata;
	}

	/**
	 * Copy default record delimiter to last non-autofilling field
	 * 
	 * @param recordMetadata
	 */
//	private void changeDefaultDelimiter(DataRecordMetadata recordMetadata) {
//		String strDelimiter = recordMetadata.getRecordDelimiterStr();
//		if (strDelimiter == null) return;
//		
//		DataFieldMetadata field = recordMetadata.getField(recordMetadata.getNumFields()-1);
//		if (!field.isAutoFilled()) {
//			return;
//		}
//		
//		for (int i=recordMetadata.getNumFields()-1; i>=0; i--) {
//			field = recordMetadata.getField(i);
//			if (!field.isAutoFilled()) {
//				if (field.getDelimiterStr() == null) {
//					field.setDelimiter(strDelimiter);
//				}
//				return;
//			}
//		}
//	}
	
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

		char type = DataFieldMetadata.str2Type(fieldType);
		if (type == DataFieldMetadata.UNKNOWN_FIELD) {
			throw new RuntimeException("Unrecognized field type specified: '" + fieldType + "'.");
		}
		return type;
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
		if (fieldSizeStr == null) {
			return 0;
		}
		return Short.parseShort(fieldSizeStr);
	}

//	/**
//	 * Description of the Method
//	 * 
//	 * @param fieldType
//	 *            Description of Parameter
//	 * @param fieldTypeOptions
//	 *            Description of Parameter
//	 * @param fieldTypeStrings
//	 *            Description of Parameter
//	 * @return Description of the Returned Value
//	 * @since May 6, 2002
//	 */
//	private final static String fieldTypeFormat(char fieldType,
//			char[] fieldTypeOptions, String[] fieldTypeStrings) {
//		for (int i = 0; i < fieldTypeOptions.length; i++) {
//			if (fieldTypeOptions[i] == fieldType) {
//				return fieldTypeStrings[i];
//			}
//		}
//		return "";
//	}

}
/*
 *  end class DataRecordMetadataXMLReaderWriter
 */
