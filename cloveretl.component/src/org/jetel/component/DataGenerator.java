
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

package org.jetel.component;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Data Generator Component</h3> <!-- Generates new records.  -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DataGenerator</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Reader</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Generates data according to pattern. Record fields can be filled by constants, random or sequence values.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0..n] - one or more output ports connected</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DATA_GENERATOR"</td></tr>
 *  <tr><td><b>pattern</b></td><td>pattern for filling new record. It is string containing 
 *  values for <b>all</b> fields, which will be not set by random or sequence values. 
 *  Field's values in this string have to have format coherent with metadata 
 *  (appropriate length or delimited by appropriate delimiter)</td></tr>
 *  <tr><td><b>recordsNumber</b></td><td>number of records to generate</td></tr>
 *  <tr><td><b>randomFields</b><br><i>optional</i></td><td>names of fields to be set 
 *  by random values (optionaly with ranges) separated by semicolon. When there are 
 *  not given random ranges (or one of them) there are used minimum possible values 
 *  for given data field (eg. for LongDataField minimum is Long.MIN_VALUE and maximum 
 *  Long.MAX_VALUE). Random strings are generated from chars 'a' till 'z'. For numeric 
 *  fields random ranges are: min value (exclusive) and max value (inclusive), 
 *  and for byte or string fields random ranges mean minimum and maximum length 
 *  of field (if it is not fixed), eg. field1=random(0,51) - for numeric field random 
 *  value from range (0,50], for string field - random string of length 0 till 51 
 *  chars, field2=random(10) - allowed only for string or byte field, means length of field  </td></tr>
 *  <tr><td><b>randomSeed</b><br><i>optional</i></td><td>Sets the seed of this random number generator using a single long seed. </td></tr>
 *  <tr><td><b>sequenceFields</b><br><i>optional</i></td><td>names of fields to be set 
 *  by values from sequence (optionaly with sequence name: fieldName=sequenceName) 
 *  separated by semicolon.</td></tr>
 *</table>
 *  <h4>Example:</h4>
 *  Metadata:
 *  <pre>
 *  &lt;Record name="Orders" type="delimited"&gt;
 *  &lt;Field delimiter="|" format="#" name="OrderID" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="CustomerID" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="EmployeeID" nullable="true" type="integer"/&gt;
 *  &lt;Field delimiter="|" format="dd.MM.yyyy" name="OrderDate" nullable="true" type="date"/&gt;
 *  &lt;Field delimiter="|" format="dd.MM.yyyy" name="RequiredDate" nullable="true" type="date"/&gt;
 *  &lt;Field delimiter="|" format="dd.MM.yyyy" name="ShippedDate" nullable="true" type="date"/&gt;
 *  &lt;Field delimiter="|" format="#" name="ShipVia" nullable="true" type="integer"/&gt;
 *  &lt;Field delimiter="|" format="#" length="8" name="Freight" nullable="true" scale="6" type="numeric"/&gt;
 *  &lt;Field delimiter="|" name="ShipName" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="ShipAddress" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="ShipCity" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="ShipRegion" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="|" name="ShipPostalCode" nullable="true" type="string"/&gt;
 *  &lt;Field delimiter="\n" name="ShipCountry" nullable="true" type="string"/&gt;
 *  &lt;/Record&gt;
 *  &lt;Node id="DATA_GENERATOR0" type="DATA_GENERATOR"&gt;
 *  &lt;attr name="randomFields"&gt;ShipAddress =random(1,777);EmployeeID=random( 1,${EMPLOYEE_NUMBER});Freight=random(1,51);ShippedDate=random(20.10.2005,30.10.2005)&lt;/attr&gt;
 *  &lt;attr name="recordsNumber"&gt;10000&lt;/attr&gt;
 *  &lt;attr name="sequenceFields"&gt;OrderID=;&lt;/attr&gt;
 *  &lt;attr name="pattern"&gt;agata|20.10.2005|30.10.2005|1|test|Prague|EU|000000|CZ
 *  &lt;/attr&gt;
 *  &lt;/Node&gt;</pre>
 *  
 *  
 *  @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 21, 2006
 *
 */
public class DataGenerator extends Node {
	
    static Log logger = LogFactory.getLog(DataGenerator.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_GENERATOR";

	/** XML attribute names */
	private static final String XML_PATTERN_ATTRIBUTE = "pattern";
	private static final String XML_RANDOM_FIELDS_ATTRIBUTE = "randomFields";
	private static final String XML_RANDOM_SEED_ATTRIBUTE = "randomSeed";
	private static final String XML_SEQUENCE_FIELDS_ATTRIBUTE = "sequenceFields";
	private static final String XML_RECORDS_NUMBER_ATTRIBUTE = "recordsNumber"; 
	
	private final int MIN = 0;
	private final int MAX = 1;
	private final int MULTIPLIER = 2; 
	private final int MOVE = 3;

	private String pattern;
	private DataRecord patternRecord;
	private Parser parser;
	private DataRecordMetadata metadata;
	private DataRecord record;
	private String[] randomFields = null;
	private String[][] randomRanges;
	private Random random;
	private long randomSeed = Long.MIN_VALUE;
	private String[] sequenceFields = null;
	private String[] sequenceIDs;
	private boolean[] randomField;//indicates if i-th field is to fill by random value
	private Object[][] specialValue;//for each field if it is not set from pattern: 0  - min random, 1 - max random, 2 - multiplier = (max random - min random)/(possible max random - possible min random),3 - move  
	private int recordsNumber;
	
	/**
	 * @param id
	 * @param pattern
	 * @param recordsNumber
	 */
	public DataGenerator(String id, String pattern, int recordsNumber) {
		super(id);
		this.pattern = pattern;
		this.recordsNumber = recordsNumber;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        super.init();
        metadata = getOutputPort(0).getMetadata();
        specialValue = new Object[metadata.getNumFields()][4];
        //create and initialize output record
		record = new DataRecord(metadata);
		record.init();
		//create metadata for pattern record - fields are set from pattern (not random and sequence values)
        DataRecordMetadata cutMetadata = metadata.duplicate();
 
        randomField = new boolean[metadata.getNumFields()];
		Arrays.fill(randomField, false);
		int randomIndex;
        int sequenceIndex;
        DataField tmpField;
        char fieldType;
        //cut random and sequence fields from pattern record
        //prepare random multiplier and move for random fields (against Random class defaults)
        //prepare sequence ID
        for (int i=0;i<metadata.getNumFields();i++){
        	randomIndex = StringUtils.findString(metadata.getField(i).getName(), 
        			randomFields);
        	if (randomIndex > -1){//field found among random fields
        		cutMetadata.delField(metadata.getField(i).getName());
        		randomField[i] = true;
        		fieldType = metadata.getField(i).getType();
        		//prepare special values for random field
        		switch (fieldType) {
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
				case DataFieldMetadata.STRING_FIELD:
				//special values mean maximum and minimum length of field
					int len = metadata.getField(i).getSize();
					if (len > 0) {
						specialValue[i][MIN] = len;
						specialValue[i][MAX] = len;
					}else{
						if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])){
							specialValue[i][MIN] = new Integer(randomRanges[randomIndex][MIN]);
						}
						if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])){
							specialValue[i][MAX] = new Integer(randomRanges[randomIndex][MAX]);
						}
						if (specialValue[i][MIN] == null){
							specialValue[i][MIN] = fieldType == DataFieldMetadata.STRING_FIELD 
							? 32 : 8 ;
						}
						if (specialValue[i][MAX] == null) {
							specialValue[i][MAX] = specialValue[i][MIN];
						}						
					}
					break;
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
				//prepare min and max from date, prepare multiplier and move	
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])){
						tmpField = record.getField(i).duplicate();
						((DateDataField)tmpField).fromString(randomRanges[randomIndex][MIN]);
						specialValue[i][MIN] = ((DateDataField)tmpField).getDate().getTime();
					}else{
						specialValue[i][MIN] = Long.MIN_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])){
						tmpField = record.getField(i).duplicate();
						((DateDataField)tmpField).fromString(randomRanges[randomIndex][MAX]);
						specialValue[i][MAX] = ((DateDataField)tmpField).getDate().getTime();
					}else{
						specialValue[i][MAX] = Long.MAX_VALUE;
					}
					//multiplier = (max - min) / (Long.Max - Long.Min)
					specialValue[i][MULTIPLIER] = (((Long) specialValue[i][MAX]).doubleValue() - ((Long) specialValue[i][MIN]).doubleValue())
					/ ((double)Long.MAX_VALUE - (double)Long.MIN_VALUE);
					//move = (min*Long.Max - max*Long.Min)/(Long.Max-Long.Min)
					specialValue[i][MOVE] = (((Long) specialValue[i][MIN]).doubleValue()*(double)Long.MAX_VALUE 
							- ((Long) specialValue[i][MAX]).doubleValue()* (double) Long.MIN_VALUE)
							/ ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
				//prepare min and max from double, multiplier = max - min, move = min, used directly in execute() method	
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])){
						specialValue[i][MIN] = new Double(randomRanges[randomIndex][MIN]);
					}else{
						specialValue[i][MIN] = -Double.MAX_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])){
						specialValue[i][MAX] = new Double(randomRanges[randomIndex][MAX]);
					}else{
						specialValue[i][MAX] = Double.MAX_VALUE;
					}
					break;
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
					//prepare min and max from integer, prepare multiplier and move	
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])){
						specialValue[i][MIN] = new Long(randomRanges[randomIndex][MIN]);
					}else{
						specialValue[i][MIN] = fieldType == DataFieldMetadata.LONG_FIELD 
							? Long.MIN_VALUE : Integer.MIN_VALUE; 
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])){
						specialValue[i][MAX] = new Long(randomRanges[randomIndex][MAX]);
					}else{
						specialValue[i][MAX] =  fieldType == DataFieldMetadata.LONG_FIELD
							? Long.MAX_VALUE : Integer.MAX_VALUE; 
					}
					//multiplier = (max - min) / (Long.Max - Long.Min)
					specialValue[i][MULTIPLIER] = (((Long) specialValue[i][MAX]).doubleValue() 
							- ((Long) specialValue[i][MIN]).doubleValue())
							/ ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE);
					//move = (min*Long.Max - max*Long.Min)/(Long.Max-Long.Min)
					specialValue[i][MOVE] = (((Long) specialValue[i][MIN]).doubleValue()*(double)Long.MAX_VALUE 
							- ((Long) specialValue[i][MAX]).doubleValue()* (double) Long.MIN_VALUE)
							/ ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE);
					break;
				default:
					throw new ComponentNotReadyException(this,"Unknown data field type " + 
							metadata.getField(i).getName() + " : " + metadata.getField(i).getTypeAsString());
				}
        	}else{//field not found among random fields
        		sequenceIndex = StringUtils.findString(metadata.getField(i).getName(), 
        				sequenceFields);
        		if (sequenceIndex > -1){//field found among sequence fields
            		cutMetadata.delField(metadata.getField(i).getName());
            		if (sequenceIDs[sequenceIndex] == null){//not given sequence id
            			//find any sequence in graph
            			specialValue[i][0]  = getGraph().getSequences().hasNext() ? (String)getGraph().getSequences().next() : null;
						if (specialValue[i][0] == null) {
							throw new ComponentNotReadyException(
									"There are no sequences defined in graph!!!");
						}            			
            		}else{
            			specialValue[i][0] = sequenceIDs[sequenceIndex];
            		}
         		}
        	}
        }
        //set random seed
		if (randomSeed > Long.MIN_VALUE) {
			random = new Random(randomSeed);
		}else{
			random = new Random();
		}
		//prepare approperiate data parser
        switch (metadata.getRecType()) {
		case DataRecordMetadata.DELIMITED_RECORD:
			parser = new DelimitedDataParser(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			break;
		case DataRecordMetadata.FIXEDLEN_RECORD:
			parser = new FixLenCharDataParser(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			break;
		default:
			parser = new DataParser(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			break;
		}
		parser.init(cutMetadata);
		try {
			parser.setDataSource(new ByteArrayInputStream(pattern.getBytes(
					Defaults.DataParser.DEFAULT_CHARSET_DECODER)));
		} catch (UnsupportedEncodingException e1) {
		}
		try {
			patternRecord = parser.getNext();
		} catch (JetelException e) {
			throw new ComponentNotReadyException(this,e);
		}
		parser.close();
	}
	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
  		 
		checkInputPorts(status, 0, 0);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);
        
        //check if on all output ports are the same metadata
        metadata = getOutputPort(0).getMetadata();
        for (int i=1;i<getOutPorts().size();i++) {
			if  (!getOutputPort(i).getMetadata().equals(metadata)){
		           ConfigurationProblem problem = new ConfigurationProblem(
		        		   "Metadata on output port nr " + i + " are not the same as on output port 0", 
		        		   ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
		            status.add(problem);
			}
		}
        
        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		boolean[] set;
		Object value = null;
		Sequence sequence;
		for (int i=0;i<recordsNumber && runIt;i++){
			//set constant fields from pattern
			set = record.copyFieldsByName(patternRecord);
			for (int j = 0; j < set.length; j++) {
				if (!set[j]){//j-th field have not been set yet 
					if (randomField[j]) {//set random value
						switch (record.getField(j).getType()) {
						case DataFieldMetadata.BYTE_FIELD:
						case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
							//create new byte array with random length (between given ranges)
							value = new byte[random
									.nextInt((Integer) specialValue[j][MAX]
											- (Integer) specialValue[j][MIN] + 1)
									+ (Integer) specialValue[j][MIN]];
							//fiil it by random bytes
							random.nextBytes((byte[])value);
							break;
						case DataFieldMetadata.DATE_FIELD:
						case DataFieldMetadata.DATETIME_FIELD:
						case DataFieldMetadata.LONG_FIELD:
						case DataFieldMetadata.INTEGER_FIELD:
							//get random long from given interval
							value = random.nextLong()
									* (Double) specialValue[j][MULTIPLIER]
									+ (Double) specialValue[j][MOVE];
							value = Math.floor(((Double)value).doubleValue());
							break;
						case DataFieldMetadata.DECIMAL_FIELD:
						case DataFieldMetadata.NUMERIC_FIELD:
							//get random double from given interval
							value = (Double)specialValue[j][MIN] + random.nextDouble()*
							((Double)specialValue[j][MAX] - (Double)specialValue[j][MIN]);
							break;
						case DataFieldMetadata.STRING_FIELD:
							//create random string of random length (between given ranges)
							value = randomString((Integer) specialValue[j][MIN],(Integer) specialValue[j][MAX]);
							break;
						}
						record.getField(j).setValue(value);
					}else {//not from pattern, not random, so sequence
						sequence = getGraph().getSequence((String)specialValue[j][0]);
						switch (record.getField(j).getType()) {
						case DataFieldMetadata.BYTE_FIELD:
						case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
						case DataFieldMetadata.STRING_FIELD:
							record.getField(j).setValue(sequence.nextValueString());
							break;
						case DataFieldMetadata.DECIMAL_FIELD:
						case DataFieldMetadata.NUMERIC_FIELD:
						case DataFieldMetadata.LONG_FIELD:
							record.getField(j).setValue(sequence.nextValueLong());
							break;
						case DataFieldMetadata.INTEGER_FIELD:
							record.getField(j).setValue(sequence.nextValueInt());
							break;
						default:
							throw new JetelException(
									"Can't set value from sequence to field "
											+ metadata.getField(j).getName()
											+ " type - "
											+ metadata.getFieldTypeAsString(j));
						}
					}
				}
			}
			writeRecordBroadcast(record);
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * This method creates random string from chars 'a' till 'z'
	 * 
	 * @param minLenght minumum length of string
	 * @param maxLenght maximum length of string
	 * @return string created from random characters. Length of this string is 
	 * between minLenght and maxLenght inclusive
	 */
	private String randomString(int minLenght,int maxLenght) {
		StringBuilder result;
		if (maxLenght != minLenght ) {
			result = new StringBuilder(random.nextInt(maxLenght - minLenght + 1)
					+ minLenght);
		}else{//minLenght == maxLenght
			result = new StringBuilder(minLenght);
		}
		for (int i = 0; i < result.capacity(); i++) {
			result.append((char)(random.nextInt('z' - 'a' + 1) + 'a'));
		}
		return result.toString();
	}
	
	/**
	 * @param graph
	 * @param nodeXML
	 * @return
	 * @throws XMLConfigurationException
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		DataGenerator dataGenerator = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			dataGenerator = new DataGenerator(xattribs.getString(XML_ID_ATTRIBUTE), 
					xattribs.getString(XML_PATTERN_ATTRIBUTE), 
					xattribs.getInteger(XML_RECORDS_NUMBER_ATTRIBUTE));
			if (xattribs.exists(XML_RANDOM_FIELDS_ATTRIBUTE)){
				dataGenerator.setRandomFields(xattribs.getString(XML_RANDOM_FIELDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_RANDOM_SEED_ATTRIBUTE)){
				dataGenerator.setRandomSeed(xattribs.getLong(XML_RANDOM_SEED_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SEQUENCE_FIELDS_ATTRIBUTE)){
				dataGenerator.setSequenceFields(xattribs.getString(XML_SEQUENCE_FIELDS_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return dataGenerator;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_RECORDS_NUMBER_ATTRIBUTE, String.valueOf(recordsNumber));
		xmlElement.setAttribute(XML_PATTERN_ATTRIBUTE, pattern);
		if (randomFields != null){
			StringBuilder fields = new StringBuilder();
			for (int i=0;i<randomFields.length;i++){
				fields.append(randomFields[i]);
				fields.append("=random(");
				fields.append(randomRanges[i][MIN]);
				fields.append(',');
				fields.append(randomRanges[i][MAX]);
				fields.append(");");
			}
		}
		if (randomSeed > Long.MIN_VALUE) {
			xmlElement.setAttribute(XML_RANDOM_SEED_ATTRIBUTE, String.valueOf(randomSeed));
		}
		if (sequenceFields != null){
			StringBuilder fields = new StringBuilder();
			for (int i=0;i<sequenceFields.length;i++){
				fields.append(sequenceFields[i]);
				fields.append("=");
				if (sequenceIDs[i] != null) {
					fields.append(sequenceIDs[i]);
				}				
				fields.append(";");
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/**
	 * Reads names of random fields with ranges from parameter and sets them 
	 * to global variables randomFields and randomRanges. If random ranges are
	 * not given sets them to empty strings
	 * 
	 * @param randomFields the randomFields to set in form fieldName=random(min,max)
	 */
	private void setRandomFields(String randomFields) {
		String[] fields = randomFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		this.randomFields = new String[fields.length];
		this.randomRanges = new String[fields.length][2];//0 - number, 1 - min, 2 - max
		String[] param;
		int leftParenthesisIndex;
		int commaIndex;
		int rightParantesisIndex;
		for (int i = 0; i < fields.length; i++) {
			param = fields[i].split("=");
			this.randomFields[i] = param[0].trim();
			if (param.length > 1){
				leftParenthesisIndex = param[1].indexOf('('); 
				commaIndex = param[1].indexOf(',');
				rightParantesisIndex = param[1].indexOf(')');
				if (commaIndex == -1) {
					randomRanges[i][MIN] = param[1].substring(leftParenthesisIndex +1,
							rightParantesisIndex);
					randomRanges[i][MAX] = "";
				}else{
					randomRanges[i][MIN] = param[1].substring(leftParenthesisIndex +1,
							commaIndex);
					randomRanges[i][MAX] = param[1].substring(commaIndex+1,
							rightParantesisIndex);
				}
				randomRanges[i][MIN] = randomRanges[i][MIN].trim();
				randomRanges[i][MAX] = randomRanges[i][MAX].trim();
			}else{
				randomRanges[i][MIN] = "";
				randomRanges[i][MAX] = "";
			}
		}
	}

	/**
	 * @param randomSeed the randomSeed to set
	 */
	private void setRandomSeed(long randomSeed) {
		this.randomSeed = randomSeed;
	}

	/**
	 * Reads names of sequence fields with sequence IDs from parameter and sets 
	 * them to global variables sequenceFields and sequenceIDs.
	 * 
	 * @param sequenceFields the sequenceFields to set in form fieldName=sequenceName or fieldName only
	 */
	private void setSequenceFields(String sequenceFields) {
		String[] fields = sequenceFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		this.sequenceFields = new String[fields.length];
		this.sequenceIDs = new String[fields.length];
		String[] param;
		for (int i = 0; i < fields.length; i++) {
			param = fields[i].split("=");
			this.sequenceFields[i] = param[0].trim();
			if (param.length > 1){
				sequenceIDs[i] = param[1].trim();
			}
		}
	}

}
