/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// FILE: c:/projects/jetel/org/jetel/metadata/DataRecordMetadataReaderWriter.java

package org.jetel.metadata;
import java.io.*;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.DOMException;
import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.SAXParseException;
import java.text.MessageFormat;
import org.jetel.util.StringUtils;

import java.util.logging.Logger;

/**
 * Helper class for reading/writing DataRecordMetadata (record structure) from/to XML format
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
 * @author     D.Pavlis
 * @since    May 6, 2002
 * @see        javax.xml.parsers
 */
public class DataRecordMetadataXMLReaderWriter extends DefaultHandler {

	// Attributes
	private DataRecordMetadata recordMetadata;
	private DocumentBuilder db;
	private DocumentBuilderFactory dbf;
	
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
		MessageFormat recordForm = new MessageFormat("<Record name=\"{0}\" type=\"{1}\"/>");
		Object[] recordArgs = {record.getName(),
				record.getRecType() == DataRecordMetadata.DELIMITED_RECORD ? "delimited" : "fixed"};

		out.println(recordForm.format(recordArgs));

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
					out.print("default=\""+field.getFormatStr()+"\" ");
				}
				out.println("/>");

			}
		}
		out.println("</Record>");
		out.flush();
	}


	private DataRecordMetadata parseRecordMetadata(Document document) throws DOMException{
		org.w3c.dom.NamedNodeMap attributes;
		DataRecordMetadata recordMetadata;
		String recordName;
		String recordType;
		// get RECORD ---------------------------------------------
		attributes = document.getElementsByTagName(RECORD_ELEMENT).item(0).getAttributes();
		
		try{
			recordName = attributes.getNamedItem("name").getNodeValue();
			recordType = attributes.getNamedItem("type").getNodeValue();
		}catch(NullPointerException ex){
			throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"name\" or \"type\" not defined within Record !");
		}
		
		recordMetadata = new DataRecordMetadata(recordName, recordType.equalsIgnoreCase("fixed") ?
					DataRecordMetadata.FIXEDLEN_RECORD :
					DataRecordMetadata.DELIMITED_RECORD);
				
		// get FIELDs ---------------------------------------------
		NodeList fieldElements = document.getElementsByTagName(FIELD_ELEMENT);
		for (int i = 0; i < fieldElements.getLength(); i++) {
			attributes = fieldElements.item(i).getAttributes();
			DataFieldMetadata field;
			String format;
			String defaultValue;
			String name;
			char fieldType;
			
			try{
				fieldType = getFieldType(attributes.getNamedItem("type").getNodeValue());
				name = attributes.getNamedItem("name").getNodeValue();
			}catch(NullPointerException ex){
				throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"name\" or \"type\" not defined for field #"+i);
			}

			// create FixLength field or Delimited base on Record Type
			try{
				if (recordMetadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
					field = new DataFieldMetadata(name, fieldType, getFieldSize(attributes.getNamedItem("size").getNodeValue()));
				} else {
					field = new DataFieldMetadata(name, fieldType, 
					StringUtils.stringToSpecChar(attributes.getNamedItem("delimiter").getNodeValue()));
				}
			}catch(NullPointerException ex){
				throw new DOMException(DOMException.NOT_FOUND_ERR,"Attribute \"size\" or \"delimiter\" not defined for field #"+i);
			}
			
			// set format string if defined
			try{
				if ((format = attributes.getNamedItem("format").getNodeValue()) != null) {
					field.setFormatStr(format);
				}
			}catch(NullPointerException ex){
			}
			
			try{
				if ((defaultValue = attributes.getNamedItem("default").getNodeValue()) != null) {
					field.setDefaultValue(defaultValue);
				}
			}catch(NullPointerException ex){
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


//	/**
//	 *  Error handler to report errors and warnings<br>
//	 *  <i>This code has beed taken from Crimson examples ;-)</i>
//	 *
//	 * @author     dpavlis
//	 * @since    October 9, 2002
//	 */
//	private static class MyErrorHandler implements ErrorHandler {
//		/**
//		 * Error handler output goes here
//		 *
//		 * @since    October 9, 2002
//		 */
//		private PrintStream out;
//
//
//		/**
//		 *Constructor for the MyErrorHandler object
//		 *
//		 * @param  out  Description of Parameter
//		 * @since       October 9, 2002
//		 */
//		MyErrorHandler(PrintStream out) {
//			this.out = out;
//		}
//
//
//		/**
//		 *  Description of the Method
//		 *
//		 * @param  spe               Description of Parameter
//		 * @exception  SAXException  Description of Exception
//		 * @since                    October 9, 2002
//		 */
//		public void warning(SAXParseException spe) throws SAXException {
//			out.println("Warning: " + getParseExceptionInfo(spe));
//		}
//
//
//		/**
//		 *  Description of the Method
//		 *
//		 * @param  spe               Description of Parameter
//		 * @exception  SAXException  Description of Exception
//		 * @since                    October 9, 2002
//		 */
//		public void error(SAXParseException spe) throws SAXException {
//			String message = "Error: " + getParseExceptionInfo(spe);
//			throw new SAXException(message);
//		}
//
//
//		/**
//		 *  Description of the Method
//		 *
//		 * @param  spe               Description of Parameter
//		 * @exception  SAXException  Description of Exception
//		 * @since                    October 9, 2002
//		 */
//		public void fatalError(SAXParseException spe) throws SAXException {
//			String message = "Fatal Error: " + getParseExceptionInfo(spe);
//			throw new SAXException(message);
//		}
//
//
//		/**
//		 * Returns a string describing parse exception details
//		 *
//		 * @param  spe  Description of Parameter
//		 * @return      The ParseExceptionInfo value
//		 * @since       October 9, 2002
//		 */
//		private String getParseExceptionInfo(SAXParseException spe) {
//			String systemId = spe.getSystemId();
//			if (systemId == null) {
//				systemId = "null";
//			}
//			String info = "URI=" + systemId +
//					" Line=" + spe.getLineNumber() +
//					": " + spe.getMessage();
//			return info;
//		}
//	}
//
}
/*
 *  end class DataRecordMetadataXMLReaderWriter
 */

