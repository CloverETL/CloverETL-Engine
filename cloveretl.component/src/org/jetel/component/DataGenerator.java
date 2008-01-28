
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.DataRecordGenerator;
import org.jetel.util.string.StringUtils;
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
 *  fields random ranges are: min value (inclusive) and max value (exclusive), 
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
	public static final String XML_RANDOM_FIELDS_ATTRIBUTE = "randomFields";
	private static final String XML_RANDOM_SEED_ATTRIBUTE = "randomSeed";
	public static final String XML_SEQUENCE_FIELDS_ATTRIBUTE = "sequenceFields";
	private static final String XML_RECORDS_NUMBER_ATTRIBUTE = "recordsNumber"; 

	private String pattern;
	private int recordsNumber;
	private DataRecordMetadata metadata;
	private long randomSeed = Long.MIN_VALUE;
	private String randomFieldsString;
	private String sequenceFieldsString;
	private DataRecordGenerator recordGenerator;
	
	private final static int WRITE_TO_PORT = 0;

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
        if(isInitialized()) return;
        super.init();
        
        metadata = getOutputPort(0).getMetadata();

        try {
            recordGenerator = new DataRecordGenerator(this, metadata, pattern, this.randomFieldsString, this.randomSeed, this.sequenceFieldsString, this.recordsNumber);
            recordGenerator.init();
        } catch (ComponentNotReadyException e){
        	throw e;
        } catch (Exception e){
			throw new ComponentNotReadyException(this, "Can't initialize record generator", e);
        }
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		recordGenerator.reset();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
  		 
		if(!checkInputPorts(status, 0, 0)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}
		
        checkMetadata(status, getOutMetadata());
        
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
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		DataRecord record = new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
		record.init();
		for (int i=0;i<recordsNumber && runIt;i++){
			 record = recordGenerator.getNext();
			writeRecordBroadcast(record);
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
					xattribs.getString(XML_PATTERN_ATTRIBUTE,""), 
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
		String randomFields = recordGenerator.getRandomFieldsString();
		if (randomFields != null)
			xmlElement.setAttribute(XML_RANDOM_FIELDS_ATTRIBUTE, randomFields);
		
		if (randomSeed > Long.MIN_VALUE) {
			xmlElement.setAttribute(XML_RANDOM_SEED_ATTRIBUTE, String.valueOf(randomSeed));
		}
		String seqFields = recordGenerator.getSequenceFieldsString();
		if (seqFields != null)
			xmlElement.setAttribute(XML_SEQUENCE_FIELDS_ATTRIBUTE, seqFields);
		
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
		this.randomFieldsString = randomFields;
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
		this.sequenceFieldsString = sequenceFields; 
	}
	
}
