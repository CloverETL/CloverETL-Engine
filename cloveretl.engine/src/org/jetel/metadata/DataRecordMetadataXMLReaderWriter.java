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
import java.io.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.Enumeration;
import java.util.logging.Logger;
import javax.xml.parsers.*;
import org.jetel.util.PropertyRefResolver;
import org.jetel.util.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.*;

/**
 * Helper class for reading/writing DataRecordMetadata (record structure) from/to XML format<br>
 *
 * <i>Note: If for record or some field Node are defined more attributes than those recognized,
 * they are transferred to Java Properties object and assigned to either record or field.</i> 
 *
 *  The XML DTD describing the internal structure is as follows:
 *  <pre>
 *  &lt;!ELEMENT Record (FIELD+)&gt;
 *  &lt;!ATTLIST Record
 *		name ID #REQUIRED
 *            	type ( fixed | delimited )  #REQUIRED &gt;
 *
 *
 *  &lt;!ELEMENT Field (#PCDATA) EMPTY&gt;
 *  &lt;!ATTLIST Field
 *            	name ID #REQUIRED
 *		type NMTOKEN #REQUIRED
 *		delimiter NMTOKEN #IMPLIED ","
 *		size NMTOKEN #IMPLIED "0"
 *		format CDATA #IMPLIED 
 *      	nullable NMTOKEN #IMPLIED "true"
 *		default CDATA #IMPLIED &gt;
 *  </pre>
 *
 *  Example:
 *  <pre>
 *   &lt;Record name="TestRecord" type="delimited"&gt;
 *	&lt;Field name="Field1" type="numeric" delimiter=";" /&gt;
 *	&lt;Field name="Field2" type="numeric" delimiter=";" /&gt;
 *	&lt;Field name="Field3" type="string" delimiter=";" /&gt;
 *	&lt;Field name="Field4" type="string" delimiter="\n" /&gt;
 *   &lt;/Record&gt;
 *  </pre>
 *
 *  If you specify your own attributes, they will be accessible
 *  through getFieldProperties() method of DataFieldMetadata class.<br> 
 *  Example:
 *  <pre>
 *  &lt;Field name="Field1" type="numeric" delimiter=";" mySpec1="1" mySpec2="xyz"/&gt;
 *  </pre>
 * @author     D.Pavlis
 * @since    May 6, 2002
 * @see        javax.xml.parsers
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

	private static final String DEFAULT_CHARACTER_ENCODING = "UTF-8";
	
	private static Logger logger = Logger.getLogger("org.jetel.metadata");
	
	// Associations

	// Operations
	/**
	 * An operation that reads DataRecord format definition from XML data
	 *
	 * @param  in  InputStream with XML data describing DataRecord
	 * @return     DataRecordMetadata object
	 * @since      May 6, 2002
	 */
	public DataRecordMetadata read(InputStream in) {
		Document document;
		
		try {
			// construct XML parser (if it is needed)
			if ((dbf == null)||(db == null)) {
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
			
			document =  db.parse(new BufferedInputStream(in)); 

		}catch(SAXParseException ex){
			logger.severe(ex.getMessage()+" --> on line "+ex.getLineNumber()+" row "+ex.getColumnNumber());			
			return null;
		}catch (ParserConfigurationException ex) {
			logger.severe(ex.getMessage());
			return null;
		}catch (Exception ex) {
			logger.severe(ex.getMessage());
			return null;
		}

		try{
			return parseRecordMetadata(document);
		}catch(DOMException ex){
			logger.severe(ex.getMessage());			
			return null;
		}catch(Exception ex){
			logger.severe("parseRecordMetadata method call: "+ex.getMessage());
			return null;
		}
	}


	/**
	 * An operation that writes DataRecord format definition into XML format
	 *
	 * @param  record     Metadata describing data record
	 * @param  outStream  OutputStream into which XML data is written
	 * @since             May 6, 2002
	 */
	public void write(DataRecordMetadata record, OutputStream outStream) {
		PrintStream out;
		Properties prop;
		try{
			out = new PrintStream(outStream,false,DEFAULT_CHARACTER_ENCODING);
		}catch(UnsupportedEncodingException ex){
			 logger.severe(ex.getMessage());
			 throw new RuntimeException(ex);
		}
		DataFieldMetadata field;

		// XML standard header
		out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

		// OUTPUT RECORD
		MessageFormat recordForm = new MessageFormat("<Record name=\"{0}\" type=\"{1}\"");
		Object[] recordArgs = {record.getName(),
				record.getRecType() == DataRecordMetadata.DELIMITED_RECORD ? "delimited" : "fixed"};

		out.print(recordForm.format(recordArgs));
		prop=record.getRecordProperties();
		if (prop!=null){
			Enumeration enumeration=prop.propertyNames();
			while(enumeration.hasMoreElements()){
				String key=(String)enumeration.nextElement();
				out.print(" "+key+"=\""+prop.get(key)+"\"");
			}
		}
		// final closing bracket
		out.println(">");

		// OUTPUT FIELDS
		MessageFormat fieldForm = new MessageFormat("\t<Field name=\"{0}\" type=\"{1}\" ");

		char[] fieldTypeLimits = {DataFieldMetadata.STRING_FIELD,
				DataFieldMetadata.DATE_FIELD,
				DataFieldMetadata.DATETIME_FIELD,
				DataFieldMetadata.NUMERIC_FIELD,
				DataFieldMetadata.INTEGER_FIELD,
				DataFieldMetadata.DECIMAL_FIELD,
				DataFieldMetadata.BYTE_FIELD};
		String[] fieldTypeParts = {"string", "date", "datetime", "numeric", "integer", "decimal","byte"};

		for (int i = 0; i < record.getNumFields(); i++) {
			field = record.getField(i);
			if (field != null) {
				Object[] fieldArgs = {field.getName(), fieldTypeFormat(field.getType(), fieldTypeLimits, fieldTypeParts)};
						
				out.print(fieldForm.format(fieldArgs));
				if (record.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
					out.print("delimiter=\"" + StringUtils.specCharToString(field.getDelimiter()) + "\" ");
				} else {
					out.print("size=\"" + field.getSize() + "\" ");
				}
				if (field.getFormatStr() != null){
					out.print("format=\""+field.getFormatStr()+"\" ");
				}
				if (field.getDefaultValue()!=null){
					out.print("default=\""+field.getDefaultValue()+"\" ");
				}
				if (field.getLocaleStr()!=null){
					out.print("locale=\""+field.getLocaleStr()+"\" ");
				}
				out.print("nullable=\""+(new Boolean(field.isNullable())).toString()+"\" ");
				
				// output field properties - if anything defined
				prop=field.getFieldProperties();
				if (prop!=null){
					Enumeration enumeration=prop.propertyNames();
					while(enumeration.hasMoreElements()){
						String key=(String)enumeration.nextElement();
						out.print(" "+key+"=\""+prop.get(key)+"\"");
					}
				}

				if (field.getCodeStr()!=null){
					out.println(">");
					out.println("\t\t<Code>");
					out.println(field.getCodeStr());
					out.println("\t\t</Code>");
					out.println("\t</Field>");
				} else {
					out.println("/>");
				}

			}
		}
		out.println("</Record>");
		out.flush();
	}


	private DataRecordMetadata parseRecordMetadata(Document document) throws DOMException{
		org.w3c.dom.NamedNodeMap attributes;
		DataRecordMetadata recordMetadata;
		String recordName=null;
		String recordType=null;
		String itemName;
		String itemValue;
		Properties recordProperties=new Properties();
		PropertyRefResolver refResolver=new PropertyRefResolver();
		// get RECORD ---------------------------------------------
		attributes = document.getElementsByTagName(RECORD_ELEMENT).item(0).getAttributes();
		
		for(int i=0;i<attributes.getLength();i++){
			itemName=attributes.item(i).getNodeName();
			itemValue=refResolver.resolveRef(attributes.item(i).getNodeValue());
			if (itemName.equalsIgnoreCase("name")){
				recordName=itemValue;
			}else if(itemName.equalsIgnoreCase("type")){
				recordType=itemValue;
			}else{
				recordProperties.setProperty(itemName,itemValue);
			}
		}
		
		if (recordType==null || recordType==null){
			throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"name\" or \"type\" not defined within Record !");
		}
		
		recordMetadata = new DataRecordMetadata(recordName, recordType.equalsIgnoreCase("fixed") ?
					DataRecordMetadata.FIXEDLEN_RECORD :
					DataRecordMetadata.DELIMITED_RECORD);
		recordMetadata.setRecordProperties(recordProperties);

		// get FIELDs ---------------------------------------------
		NodeList fieldElements = document.getElementsByTagName(FIELD_ELEMENT);
		for (int i = 0; i < fieldElements.getLength(); i++) {
			attributes = fieldElements.item(i).getAttributes();
			DataFieldMetadata field;
			String format=null;
			String defaultValue=null;
			String name=null;
			String size=null;
			String delimiter=null;
			String nullable=null;
			String localeStr=null;
			char fieldType=' ';
			Properties fieldProperties=null;
			
			for(int j=0;j<attributes.getLength();j++){
				itemName=attributes.item(j).getNodeName();
				itemValue=refResolver.resolveRef(attributes.item(j).getNodeValue());
				if (itemName.equalsIgnoreCase("type")){
					fieldType = getFieldType(itemValue);
				}else if(itemName.equalsIgnoreCase("name")){
					name = itemValue;
				}else if(itemName.equalsIgnoreCase("size")){
					size = itemValue;
				}else if(itemName.equalsIgnoreCase("delimiter")){
					delimiter = itemValue;
				}else if(itemName.equalsIgnoreCase("format")){
					format = itemValue;
				}else if(itemName.equalsIgnoreCase("default")){
					defaultValue = itemValue;
				}else if(itemName.equalsIgnoreCase("nullable")){
					nullable = itemValue;
				}else if(itemName.equalsIgnoreCase("locale")){
					localeStr = itemValue;
				}else{
					if(fieldProperties==null){
						fieldProperties=new Properties();
					}
					fieldProperties.setProperty(itemName,itemValue);
				}
			}
			
			if ((fieldType==' ') || (name==null)){
				throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"name\" or \"type\" not defined for field #"+i);
			}

			// create FixLength field or Delimited base on Record Type
			if (recordMetadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
				if (size==null){
					throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"size\" not defined for field #"+i);
				}
				field = new DataFieldMetadata(name, fieldType, getFieldSize(size));
			} else {
				if (delimiter==null){
					throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"delimiter\" not defined for field #"+i);
				}
				field = new DataFieldMetadata(name, fieldType,StringUtils.stringToSpecChar(delimiter));
			}
			// set properties
			field.setFieldProperties(fieldProperties);
			
			// set format string if defined
			if (format  != null) {
				field.setFormatStr(format);
			}
			if (defaultValue  != null) {
				field.setDefaultValue(defaultValue);
			}
			
			// set nullable if defined
			if (nullable != null) {
				field.setNullable(nullable.matches("^[tTyY].*"));
			}
			// set localeStr if defined
			if (localeStr!=null){
				field.setLocaleStr(localeStr);
			}
				
			NodeList fieldCodeElements = fieldElements.item(i).getChildNodes();
			if( fieldCodeElements != null ) {
				for(int l = 0; l < fieldCodeElements.getLength() ; l++) {
					if(fieldCodeElements.item( l ).getNodeName().equals(CODE_ELEMENT)) {
						field.setCodeStr(fieldCodeElements.item( l ).getFirstChild().getNodeValue());
					}
				}
			}
			recordMetadata.addField(field);
		}
		return recordMetadata;
	}
	
	
	/**
	 *  Gets the FieldType attribute of the DataRecordMetadataXMLReaderWriter object
	 *
	 * @param  fieldType         Description of Parameter
	 * @return                   The FieldType value
	 * @since                    May 6, 2002
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
		if (fieldType.equalsIgnoreCase("decimal")) {
			return DataFieldMetadata.DECIMAL_FIELD;
		}
		if (fieldType.equalsIgnoreCase("byte")) {
			return DataFieldMetadata.BYTE_FIELD;
		}

		throw new RuntimeException("Unrecognized field type specified!");
	}


	/**
	 *  Gets the FieldSize attribute of the DataRecordMetadataXMLReaderWriter object
	 *
	 * @param  fieldSizeStr      Description of Parameter
	 * @return                   The FieldSize value
	 * @exception  SAXException  Description of Exception
	 * @since                    May 6, 2002
	 */
	private short getFieldSize(String fieldSizeStr){
		return Short.parseShort(fieldSizeStr);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  fieldType         Description of Parameter
	 * @param  fieldTypeOptions  Description of Parameter
	 * @param  fieldTypeStrings  Description of Parameter
	 * @return                   Description of the Returned Value
	 * @since                    May 6, 2002
	 */
	private final static String fieldTypeFormat(char fieldType, char[] fieldTypeOptions, String[] fieldTypeStrings) {
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

