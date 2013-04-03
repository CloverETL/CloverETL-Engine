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
package org.jetel.component;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.Result;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.DataRecordGenerator;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

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
 *  <tr><td><b>randomFields</b><br><i>optional</i></td><td>names of fields (can be preceded by Defaults.CLOVER_FIELD_INDICATOR - $)
 *  to be set by random values (optionaly with ranges) separated by :;| {colon, semicolon, pipe}. When there are 
 *  not given random ranges (or one of them) there are used minimum possible values 
 *  for given data field (eg. for LongDataField minimum is Long.MIN_VALUE and maximum 
 *  Long.MAX_VALUE). Random strings are generated from chars 'a' till 'z'. For numeric 
 *  fields random ranges are: min value (inclusive) and max value (exclusive), 
 *  and for byte or string fields random ranges mean minimum and maximum length 
 *  of field (if it is not fixed), eg. $field1:=random(0,51) - for numeric field random 
 *  value from range (0,50], for string field - random string of length 0 till 51 
 *  chars, $field2:=random(10) - allowed only for string or byte field, means length of field  </td></tr>
 *  <tr><td><b>randomSeed</b><br><i>optional</i></td><td>Sets the seed of this random number generator using a single long seed. </td></tr>
 *  <tr><td><b>sequenceFields</b><br><i>optional</i></td><td>names of fields (can be preceded by Defaults.CLOVER_FIELD_INDICATOR - $) 
 *  to be set by values from sequence (optionaly with sequence name: $fieldName:=sequenceName) 
 *  separated by :;| {colon, semicolon, pipe}.</td></tr>
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
 *  &lt;Field delimiter="|" format="#" length="8" name="Freight" nullable="true" scale="6" type="number"/&gt;
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
public class SimpleDataGenerator extends DataGenerator {
	
    static Log logger = LogFactory.getLog(SimpleDataGenerator.class);

	/** XML attribute names */
	private static final String XML_PATTERN_ATTRIBUTE = "pattern";
	public static final String XML_RANDOM_FIELDS_ATTRIBUTE = "randomFields";
	public static final String XML_SEQUENCE_FIELDS_ATTRIBUTE = "sequenceFields";

	private String pattern;
	private DataRecordMetadata metadata;
	private String randomFieldsString;
	private String sequenceFieldsString;
	private DataRecordGenerator recordGenerator;

	/**
	 * @param id
	 * @param pattern
	 * @param recordsNumber
	 */
	public SimpleDataGenerator(String id, String pattern, long recordsNumber) {
		super(id);
		this.pattern = pattern;
		this.recordsNumber = recordsNumber;
	}

	@Override
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
        
   		autoFilling.addAutoFillingFields(metadata);
   		autoFilling.setFilename(getId());
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		recordGenerator.reset();
    		autoFilling.reset();
    	}
    }    

	@Override
	public Result execute() throws Exception {
		DataRecord record = DataRecordFactory.newRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
		record.init();
		for (long i=0;i<recordsNumber && runIt;i++){
			record = recordGenerator.getNext(record);
			autoFilling.setAutoFillingFields(record);
			writeRecordBroadcast(record);
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
  		 
		if(!checkInputPorts(status, 0, 0)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}
        checkMetadata(status, getOutMetadata());
        
        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
	}

	@Override
	public synchronized void free() {
	    super.free();
	}

	/**
	 * Reads names of random fields with ranges from parameter and sets them 
	 * to global variables randomFields and randomRanges. If random ranges are
	 * not given sets them to empty strings
	 * 
	 * @param randomFields the randomFields to set in form fieldName=random(min,max)
	 */
	public void setRandomFields(String randomFields) {
		this.randomFieldsString = randomFields;
	}

	/**
	 * Reads names of sequence fields with sequence IDs from parameter and sets 
	 * them to global variables sequenceFields and sequenceIDs.
	 * 
	 * @param sequenceFields the sequenceFields to set in form fieldName=sequenceName or fieldName only
	 */
	public void setSequenceFields(String sequenceFields) {
		this.sequenceFieldsString = sequenceFields; 
	}
}
