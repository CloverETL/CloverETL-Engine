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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecordNature;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.XmlParserFactory;
import org.jetel.util.XmlParserFactory.DocumentBuilderProvider;
import org.jetel.util.KeyFieldNamesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
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
	private static DocumentBuilderFactory documentBuilderFactory;

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
	public static final String NAME_ATTR = "name"; 
	public static final String LABEL_ATTR = "label"; 
    public static final String TYPE_ATTR = "type";
    public static final String CONTAINER_TYPE_ATTR = "containerType";
    private static final String RECORD_SIZE_ATTR = "recordSize";
    public  static final String RECORD_DELIMITER_ATTR = "recordDelimiter";
    public  static final String FIELD_DELIMITER_ATTR = "fieldDelimiter";
	public static final String DELIMITER_ATTR = "delimiter";
	public static final String EOF_AS_DELIMITER_ATTR = "eofAsDelimiter";
	public static final String FORMAT_ATTR = "format";
	public static final String DEFAULT_ATTR = "default";
	public static final String LOCALE_ATTR = "locale";
	public static final String TIMEZONE_ATTR = "timeZone";
	public static final String NULLABLE_ATTR = "nullable";
	public static final String NULL_VALUE_ATTR = "nullValue";
	private static final String COMPRESSED_ATTR = "compressed";
	private static final String SHIFT_ATTR = "shift";
	public static final String SIZE_ATTR = "size";
	public static final String TRIM_ATTR = "trim";
	private static final String SKIP_SOURCE_ROW_ATTR = "skipSourceRows";
	public static final String QUOTED_STRINGS = "quotedStrings";
	public static final String QUOTE_CHAR = "quoteChar";
	public static final String KEY_FIELD_NAMES_ATTR = "keyFieldNames";
	private static final String AUTO_FILLING_ATTR = "auto_filling";
	public static final String DESCRIPTION_ATTR = "description";
	public static final String CONNECTION_ATTR = "connection";
	private static final String COLLATOR_SENSITIVITY_ATTR = "collator_sensitivity";
	private static final String NATURE_ATTR = "nature";
	
	/** Default encoding for XML representation of metadata. */
	public static final String DEFAULT_CHARACTER_ENCODING = "UTF-8";

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
        this((GraphParameters) null);
    }

    /**
     * Constructor with graph to property resolving.
     * @param graph
     */
    public DataRecordMetadataXMLReaderWriter(TransformationGraph graph) {
        this(graph.getGraphParameters());
    }
    
    /**
     * Constructor with properties for resolving.
     * @param properties
     */
    public DataRecordMetadataXMLReaderWriter(GraphParameters graphParameters) {
        refResolver = new PropertyRefResolver(graphParameters);
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
	
	private static synchronized DocumentBuilderProvider getDocumentBuilderProvider() {
		if (documentBuilderFactory == null) {
			documentBuilderFactory = DocumentBuilderFactory.newInstance();

			documentBuilderFactory.setNamespaceAware(true);
			documentBuilderFactory.setCoalescing(true);

			// Optional: set various configuration options
			documentBuilderFactory.setValidating(validation);
			documentBuilderFactory.setIgnoringComments(ignoreComments);
			documentBuilderFactory.setIgnoringElementContentWhitespace(ignoreWhitespaces);
			documentBuilderFactory.setCoalescing(putCDATAIntoText);
			documentBuilderFactory.setExpandEntityReferences(!createEntityRefs);
		}
		
		return XmlParserFactory.getDocumentBuilder(documentBuilderFactory);
	}
	
	public DataRecordMetadata read(InputStream in, String metadataId) {
		Document document;

		DocumentBuilderProvider documentBuilderProvider = null;
		try {
			documentBuilderProvider = getDocumentBuilderProvider();
			document = documentBuilderProvider.getDocumentBuilder().parse(new BufferedInputStream(in));
			document.normalize();
		} catch (SAXParseException ex) {
			logger.error("SAX Exception on line "
					+ ex.getLineNumber() + " row " + ex.getColumnNumber(), ex);
			return null;
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			XmlParserFactory.releaseDocumentBuilder(documentBuilderProvider);
		}

		try {
			return parseRecordMetadata(document, metadataId);
		} catch (DOMException ex) {
			logger.error(ex);
			return null;
		} catch (Exception ex) {
			logger.error("parseRecordMetadata method call", ex);
			return null;
		}
	}

	/**
	 * Parses {@link DataRecordMetadata} from given XML.
	 * Root element of the input should be "Metadata" element.
	 * @param xmlElement
	 * @return metadata defined by given xml element
	 */
	public static DataRecordMetadata read(Element xmlElement) {
		DataRecordMetadataXMLReaderWriter reader = new DataRecordMetadataXMLReaderWriter();
		return reader.parseMetadata(xmlElement);
	}

	public static DataRecordMetadata readMetadata(InputStream input) {
		DataRecordMetadataXMLReaderWriter reader = new DataRecordMetadataXMLReaderWriter();
		return reader.read(input);
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
            throw new JetelRuntimeException(e);
        }
	}
	
	public static void write(DataRecordMetadata record, Element metadataElement) {
		String rt;
		DataFieldMetadata field;

		// Record - Basic properties
		metadataElement.setAttribute(NAME_ATTR, record.getName());
		
		String label = record.getLabel();
		if (!StringUtils.isEmpty(label)) {
			metadataElement.setAttribute(LABEL_ATTR, StringUtils.specCharToString(label));
		}

		rt = toString(record.getParsingType());
        metadataElement.setAttribute(TYPE_ATTR, rt);

        if (record.isSpecifiedRecordDelimiter()) {
            metadataElement.setAttribute(RECORD_DELIMITER_ATTR, StringUtils.specCharToString(record.getRecordDelimiter()));
        }
        if (record.isSpecifiedFieldDelimiter()) {
        	metadataElement.setAttribute(FIELD_DELIMITER_ATTR, StringUtils.specCharToString(record.getFieldDelimiter()));
        }
        if (record.getSkipSourceRows() > 0) {
        	metadataElement.setAttribute(SKIP_SOURCE_ROW_ATTR, String.valueOf(record.getSkipSourceRows()));
        }
        if (record.getRecordSize() > 0) {
            metadataElement.setAttribute(RECORD_SIZE_ATTR, String.valueOf(record.getRecordSize()));
        }
        if (record.getDescription() != null) {
            metadataElement.setAttribute(DESCRIPTION_ATTR, record.getDescription());
        }
        // Record - Advanced properties

        if (record.isQuotedStrings()) {
        	metadataElement.setAttribute(QUOTED_STRINGS, String.valueOf(record.isQuotedStrings()));
        }
        if (record.getQuoteChar() != null) {
        	metadataElement.setAttribute(QUOTE_CHAR, String.valueOf(record.getQuoteChar()));
        }
        if (record.getLocaleStr() != null) {
        	metadataElement.setAttribute(LOCALE_ATTR, record.getLocaleStr());
		}
        if (record.getCollatorSensitivity() != null) {
        	metadataElement.setAttribute(COLLATOR_SENSITIVITY_ATTR, record.getCollatorSensitivity());
        }
        if (record.getTimeZoneStr() != null) {
        	metadataElement.setAttribute(TIMEZONE_ATTR, record.getTimeZoneStr());
        }
        if (record.getNullValues() != DataRecordMetadata.DEFAULT_NULL_VALUES) {
        	metadataElement.setAttribute(NULL_VALUE_ATTR, StringUtils.join(record.getNullValues(), Defaults.DataFormatter.DELIMITER_DELIMITERS));
        }
        if (record.getKeyFieldNames() != null && !record.getKeyFieldNames().isEmpty()) {
        	metadataElement.setAttribute(KEY_FIELD_NAMES_ATTR, KeyFieldNamesUtils.getFieldNamesAsString(record.getKeyFieldNames()));
        }
        if (record.getEofAsDelimiter() != null) {
        	metadataElement.setAttribute(EOF_AS_DELIMITER_ATTR, String.valueOf(record.getEofAsDelimiter()));
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

				// Field - Basic properties
				fieldElement.setAttribute(NAME_ATTR, field.getName());
				label = field.getLabel();
				if (!StringUtils.isEmpty(label)) {
				    fieldElement.setAttribute(LABEL_ATTR, StringUtils.specCharToString(label));
				}
			    fieldElement.setAttribute(TYPE_ATTR, field.getDataType().getName());
			    if (field.getContainerType() != null && field.getContainerType() != DataFieldContainerType.SINGLE) {
			    	fieldElement.setAttribute(CONTAINER_TYPE_ATTR, field.getContainerType().toString());
			    }

				if (record.getParsingType() == DataRecordParsingType.DELIMITED 
						&& !StringUtils.isEmpty(delimiterStr)) {
					fieldElement.setAttribute(DELIMITER_ATTR, delimiterStr);
				} else {
					if (field.getSize() != 0) {
						fieldElement.setAttribute(SIZE_ATTR, String.valueOf(field.getSize()));
					}
				}
				if (!field.isNullable()) {
					fieldElement.setAttribute(NULLABLE_ATTR, String.valueOf(field.isNullable()));
				}
				if (field.getDefaultValueStr() != null) {
					fieldElement.setAttribute(DEFAULT_ATTR, field.getDefaultValueStr());
				}
				if (field.getDescription() != null) {
					fieldElement.setAttribute(DESCRIPTION_ATTR, field.getDescription());
				}
				
				// Field - Advanced properties
				if (field.hasFormat()) {
					fieldElement.setAttribute(FORMAT_ATTR, field.getFormatStr());
				}
				if (field.getDefaultValueStr() != null) {
					fieldElement.setAttribute(DEFAULT_ATTR, field.getDefaultValueStr());
				}
				if (field.getLocaleStr() != null) {
					fieldElement.setAttribute(LOCALE_ATTR, field.getLocaleStrFieldOnly());
				}
				if (field.getCollatorSensitivity() != null) {
					fieldElement.setAttribute(COLLATOR_SENSITIVITY_ATTR, field.getCollatorSensitivityFieldOnly());
				}
				if (field.getNullValuesOnField() != null) {
					fieldElement.setAttribute(NULL_VALUE_ATTR, StringUtils.join(field.getNullValuesOnField(), Defaults.DataFormatter.DELIMITER_DELIMITERS));
				}
				if (field.getTrim() != null) {
					fieldElement.setAttribute(TRIM_ATTR, String.valueOf(field.getTrim()));
				}
				if (field.getTimeZoneStr() != null) {
					fieldElement.setAttribute(TIMEZONE_ATTR, field.getTimeZoneStrFieldOnly());
				}
				if (field.getAutoFilling() != null) {
					fieldElement.setAttribute(AUTO_FILLING_ATTR, field.getAutoFilling());
				}
				if (field.getShift() != 0) {
			    	fieldElement.setAttribute(SHIFT_ATTR, String.valueOf(field.getShift()));
			    }
				if (field.isEofAsDelimiter()) {
					fieldElement.setAttribute(EOF_AS_DELIMITER_ATTR, String.valueOf(field.isEofAsDelimiter()));
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
	
	/**
	 * Parsers metadata from given xml.
	 * Root element should be "Metadata" element.
	 * @param element
	 * @return
	 */
	public DataRecordMetadata parseMetadata(Element element) {
		String id = element.getAttribute(ID);
		if (!StringUtils.isEmpty(id)) {
			NodeList childElements = element.getElementsByTagName(RECORD_ELEMENT);
			if (childElements.getLength() == 1) {
				DataRecordMetadata metadata = parseRecordMetadata(childElements.item(0));
				metadata.setId(id);
				return metadata;
			} else {
				throw new JetelRuntimeException("Invalid metadata (" + id + ") format.");
			}
		} else {
			throw new JetelRuntimeException("Missing metadata ID.");
		}
		
	}
	public DataRecordMetadata parseRecordMetadata(org.w3c.dom.Node topNode) throws DOMException {
		if (topNode == null || !RECORD_ELEMENT.equals(topNode.getNodeName())) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"Root node is not of type " + RECORD_ELEMENT);
		}

		org.w3c.dom.NamedNodeMap attributes;
		DataRecordMetadata recordMetadata;
		
		// Record - Basic properties
		String recordName = null;
		String recordLabel = null;
		String recordType = null;
		String recordDelimiter = null;
		String fieldDelimiter = null;
		String skipSourceRows = null;
		String description = null;
		String sizeStr = null;
		
		// Record - Advanced properties
		String quotedStrings = null;
		String quoteChar = null;
		String recLocaleStr = null;
		String collatorSensitivity = null;
		String recTimeZoneStr = null;
		String recNullValue = null;
		String keyFieldNamesStr = null;
		String eofAsDelimiter = null;
		
		// ??? deprecated ???
		
		String nature = null;
		
		String itemName;
		String itemValue;
		
		Properties recordProperties = null;

		/*
		 * get Record level metadata
		 */
		attributes = topNode.getAttributes();
		for (int i = 0; i < attributes.getLength(); i++) {
			itemName = attributes.item(i).getNodeName();
			itemValue = refResolver.resolveRef(attributes.item(i)
					.getNodeValue());
			
			// Record - Basic properties
			if (itemName.equalsIgnoreCase(NAME_ATTR)) {
				recordName = itemValue;
			} else if (itemName.equalsIgnoreCase(LABEL_ATTR)) {
				recordLabel = itemValue;
			} else if (itemName.equalsIgnoreCase(TYPE_ATTR)) {
				recordType = itemValue;
			} else if (itemName.equalsIgnoreCase(RECORD_DELIMITER_ATTR)) {
				recordDelimiter = itemValue;
			} else if (itemName.equalsIgnoreCase(FIELD_DELIMITER_ATTR)) {
				fieldDelimiter = itemValue;
			} else if (itemName.equalsIgnoreCase(SKIP_SOURCE_ROW_ATTR)) {
				skipSourceRows = itemValue;
			} else if (itemName.equalsIgnoreCase(DESCRIPTION_ATTR)) {
				description = itemValue;
			} else if (itemName.equalsIgnoreCase(RECORD_SIZE_ATTR)) { // visible only in fixed length records
				sizeStr = itemValue;
				
			// Record - Advanced properties
			} else if (itemName.equalsIgnoreCase(QUOTED_STRINGS)) {
				quotedStrings = itemValue;
			} else if (itemName.equalsIgnoreCase(QUOTE_CHAR)) {
				quoteChar = itemValue;
			} else if (itemName.equalsIgnoreCase(LOCALE_ATTR)) {
				recLocaleStr = itemValue;
			} else if (itemName.equalsIgnoreCase(COLLATOR_SENSITIVITY_ATTR)) {
				collatorSensitivity = itemValue;
			} else if (itemName.equalsIgnoreCase(TIMEZONE_ATTR)) {
				recTimeZoneStr = itemValue;
			} else if (itemName.equalsIgnoreCase(NULL_VALUE_ATTR)) {
				recNullValue = itemValue;
			} else if (itemName.equalsIgnoreCase(KEY_FIELD_NAMES_ATTR)) {
				keyFieldNamesStr = itemValue;
			} else if (itemName.equalsIgnoreCase(EOF_AS_DELIMITER_ATTR)) {
				eofAsDelimiter = itemValue;
				
			// ??? deprecated ???
			} else if (itemName.equalsIgnoreCase(NATURE_ATTR)) {
				nature = itemValue;
			} else {
				if (recordProperties == null) {
					recordProperties = new Properties();
				}
				recordProperties.setProperty(itemName, itemValue);
			}
		}

		// Record - Basic properties
		if (recordType == null || recordName == null) {
			throw new DOMException(DOMException.NOT_FOUND_ERR,
					"Attribute \"name\" or \"type\" not defined within Record !");
		}

		DataRecordParsingType rt = DataRecordParsingType.fromString(recordType);

		recordMetadata = new DataRecordMetadata(recordName, rt);
		
		if (!StringUtils.isEmpty(recordLabel)) {
			recordMetadata.setLabel(recordLabel);
		}
		if (!StringUtils.isEmpty(recordDelimiter)) {
			recordMetadata.setRecordDelimiter(recordDelimiter);
		}
		if (!StringUtils.isEmpty(fieldDelimiter)) {
			recordMetadata.setFieldDelimiter(fieldDelimiter);
		}
		
		try {
			int iSkipSourceRows = -1;
			if (skipSourceRows != null)	iSkipSourceRows = Integer.parseInt(skipSourceRows);
			recordMetadata.setSkipSourceRows(iSkipSourceRows);
		} catch (NumberFormatException e) {
			// ignore 
		}
		
		if (!StringUtils.isEmpty(description)) {
			recordMetadata.setDescription(description);
		}
		
		int recSize = 0;
		try {
			recSize = Integer.parseInt(sizeStr);
		} catch (NumberFormatException e) {
			// ignore 
		}
		recordMetadata.setRecordSize(recSize);
		
		
		// Record - Advanced properties
		if (!StringUtils.isEmpty(quoteChar)) {
			recordMetadata.setQuoteChar(QuotingDecoder.quoteCharFromString(quoteChar));
		}
		if (!StringUtils.isEmpty(quotedStrings)) {
			recordMetadata.setQuotedStrings(Boolean.valueOf(quotedStrings));
		}
		if (recLocaleStr != null) {
			recordMetadata.setLocaleStr(recLocaleStr);
		}
		if (!StringUtils.isEmpty(collatorSensitivity)) {
			recordMetadata.setCollatorSensitivity(collatorSensitivity);
		}
		if (recTimeZoneStr != null) {
			recordMetadata.setTimeZoneStr(recTimeZoneStr);
		}
		if (recNullValue != null) {
			recordMetadata.setNullValues(Arrays.asList(recNullValue.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX, -1)));
		}
		if (!StringUtils.isEmpty(keyFieldNamesStr)) {
			recordMetadata.setKeyFieldNames(KeyFieldNamesUtils.getFieldNamesAsList(keyFieldNamesStr));
		}
		if (!StringUtils.isEmpty(eofAsDelimiter)) {
			recordMetadata.setEofAsDelimiter(Boolean.valueOf(eofAsDelimiter));
		}
		
		// ??? deprecated ???
		if (!StringUtils.isEmpty(nature)) {
			recordMetadata.setNature(DataRecordNature.fromString(nature));
		}
		
		recordMetadata.setRecordProperties(recordProperties);

		/*
		 * parse metadata of FIELDs
		 */

		NodeList fieldElements = topNode.getChildNodes();
		for (int i = 0; i < fieldElements.getLength(); i++) {
			String fieldElementName = fieldElements.item(i).getNodeName();
			if (!fieldElementName.equals(FIELD_ELEMENT)) {
				
				// description attribute of the record:
				// description is serialized like this: <attr name="description"><![CDATA[my description of this record]]></attr>
				if (fieldElementName.equalsIgnoreCase("attr")) {
					Node nameAttr = fieldElements.item(i).getAttributes().getNamedItem("name");
					if (nameAttr != null && nameAttr.getNodeValue().equalsIgnoreCase(DESCRIPTION_ATTR)) {
						description = fieldElements.item(i).getFirstChild().getNodeValue();
					}
					recordMetadata.setDescription(description);
				}
				continue;
			}
			
			attributes = fieldElements.item(i).getAttributes();
			DataFieldMetadata field;
			// Field - Basic properties
			String name = null;
			String label = null;
			char fieldType = ' ';
			DataFieldContainerType containerType = null;
			String size = null;
			String nullable = null;
			String delimiter = null;
			String defaultValue = null;
			/*String*/ description = null;
			
			// Field - Advanced properties
			String format = null;
			String localeStr = null;
			/*String*/ collatorSensitivity = null;
			String timeZoneStr = null;
			String nullValue = null;
			String trim = null;
			String autoFilling = null;
			String shift = null;
			/*String*/ eofAsDelimiter = null;
			
			// ??? deprecated ???
			String compressed = null;
			
			Properties fieldProperties = new Properties();

			for (int j = 0; j < attributes.getLength(); j++) {
				itemName = attributes.item(j).getNodeName();
                    itemValue = refResolver.resolveRef(attributes.item(j)
                            .getNodeValue());
                    
                // Field - Basic attributes
				if (itemName.equalsIgnoreCase(TYPE_ATTR)) {
					fieldType = getFieldType(itemValue);
				} else if (itemName.equalsIgnoreCase(CONTAINER_TYPE_ATTR)) {
					containerType = DataFieldContainerType.fromString(itemValue);
				} else if (itemName.equalsIgnoreCase(NAME_ATTR)) {
					name = itemValue;
				} else if (itemName.equalsIgnoreCase(LABEL_ATTR)) {
					label = itemValue;
				} else if (itemName.equalsIgnoreCase(SIZE_ATTR)) {
					size = itemValue;
				} else if (itemName.equalsIgnoreCase(DELIMITER_ATTR)) {
					delimiter = itemValue;
				} else if (itemName.equalsIgnoreCase(DEFAULT_ATTR)) {
					defaultValue = itemValue;
				} else if (itemName.equalsIgnoreCase(NULLABLE_ATTR)) {
					nullable = itemValue;
				} else if (itemName.equalsIgnoreCase(DESCRIPTION_ATTR)) {
					description = itemValue;
					
				// Field - Advanced attributes
				} else if (itemName.equalsIgnoreCase(FORMAT_ATTR)) {
					format = itemValue;
				} else if (itemName.equalsIgnoreCase(LOCALE_ATTR)) {
					localeStr = itemValue;
				} else if (itemName.equalsIgnoreCase(COLLATOR_SENSITIVITY_ATTR)) {
					collatorSensitivity = itemValue;
				} else if (itemName.equalsIgnoreCase(TIMEZONE_ATTR)) {
					timeZoneStr = itemValue;
				} else if (itemName.equalsIgnoreCase(NULL_VALUE_ATTR)) {
					nullValue = itemValue;
				} else if (itemName.equalsIgnoreCase(TRIM_ATTR)) {
					trim = itemValue;
				} else if (itemName.equalsIgnoreCase(AUTO_FILLING_ATTR)) {
					autoFilling = itemValue;
				} else if (itemName.equalsIgnoreCase(SHIFT_ATTR)) {
					shift = itemValue;
				} else if (itemName.equalsIgnoreCase(EOF_AS_DELIMITER_ATTR)) {
					eofAsDelimiter = itemValue;
					
				// ??? deprecated ???
				} else if (itemName.equalsIgnoreCase(COMPRESSED_ATTR)) {
					compressed = itemValue;
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
				field = new DataFieldMetadata(name, fieldType, getFieldSize(size));
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
			 
			try{
				//delimited&mixed field still can have size;e.g. when extracted from DB, just for info
				int sizeVal=getFieldSize(size);
				if (sizeVal > 0) field.setSize(sizeVal);
			}catch(NumberFormatException ex){
					//ignore, if we don't get size, then it's not important
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
			// set the description
			if (description != null) {
				field.setDescription(description);
			}

			// set nullable if defined
			if (nullable != null) {
				field.setNullable(nullable.matches("^[tTyY].*"));
			}

			if (nullValue != null) {
				field.setNullValues(Arrays.asList(nullValue.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX, -1)));
			}

			// set localeStr if defined
			if (localeStr != null) {
				field.setLocaleStr(localeStr);
			}
			
			// set timeZoneStr if defined
			if (timeZoneStr != null) {
				field.setTimeZoneStr(timeZoneStr);
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
			
			// description attribute of the fields:
			NodeList fieldChildren = fieldElements.item(i).getChildNodes();
			for (int j = 0; j < fieldChildren.getLength(); j++) {
				String fieldChildElementName = fieldChildren.item(j).getNodeName();
				if (fieldChildElementName.equalsIgnoreCase("attr")) {
					Node nameAttr = fieldChildren.item(j).getAttributes().getNamedItem("name");
					if (nameAttr != null && nameAttr.getNodeValue().equalsIgnoreCase(DESCRIPTION_ATTR)) {
						description = fieldChildren.item(j).getFirstChild().getNodeValue();
					}
					field.setDescription(description);
				}
			}
			
			recordMetadata.addFieldFast(field);
		}
		
		recordMetadata.structureChanged();
		
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
	private int getFieldSize(String fieldSizeStr) {
		if (StringUtils.isEmpty(fieldSizeStr)) {
			return 0;
		}
		return Integer.parseInt(fieldSizeStr);
	}

	/**
	 * Converts {@link DataRecordParsingType} to its name,
	 * as used in metadata XML.
	 * 
	 * @param type
	 * @return [delimited|fixed|mixed]
	 */
	public static String toString(DataRecordParsingType type) {
		if (type == DataRecordParsingType.DELIMITED) {
			return "delimited";
		} else if (type == DataRecordParsingType.FIXEDLEN) {
			return "fixed";
		} else {
			return "mixed";
		}
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
