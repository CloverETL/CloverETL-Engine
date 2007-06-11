
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

import java.util.Arrays;
import java.util.List;

import org.jetel.component.aggregate.AggregateMappingParser;
import org.jetel.component.aggregate.AggregationException;
import org.jetel.component.aggregate.AggregateMappingParser.ConstantMapping;
import org.jetel.component.aggregate.AggregateMappingParser.FieldMapping;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>MemSeqNoIncrementer Component</h3>
 *
 * <!-- This component increments value of given field by one if one of the key fields is not null. Records  with changed  MemSeqNo are reformated according to given formula.-->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>MemSeqNoIncrementer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>transformers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component increments value of given field by one if one of the key fields is not null. Records  with changed  MemSeqNo are reformated according to given formula.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - reformated records, for which MemSeqNo was changed. When at least one of the key fields is not null, value of field set as „memseqnoField” is incremented by one, record is reformated according to the formula given by „mapping” attribute and sent to port 0.<br>
 * 	   [1] - records with the same structure as on input, but with modified MemSeqNo. Copy of input record, but the field set as „memseqnoField” can be incremented by one (if  at least one of the key fields is not null)
 * </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MEMSEQNO_INCREMENT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>memseqnoField</b></td><td>field to increment</td>
 *  <tr><td><b>notnullKey</b></td><td>field names separated by :;| {colon, semicolon, pipe} to check if are not null</td>
 *  <tr><td><b>mapping</b></td><td>pairs ($p1:=x) of output field names preceeded by $ and and theirs values separated by :;| {colon, semicolon, pipe}. To output field it can be mapped input  field (preceeded by $) or constant. </td>
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>  &lt;Node id="MEMSECNO_INCREMENT1" type="MEMSECNO_INCREMENT" memseqnoField=” Memseqno” notnullKey=”StreatAddress;City;State;ZipCode”&gt;
	  &lt;attr name=”mapping”&gt;&lt;![CDATA[$Memrecno:=$Id;$Memseqno=$Memseqno;$Caudrecno=10;$Maudrecno=10;$Recstat=”A”;$Attrrecno=102;
		$Stline1=$StreatAddress;$City=$City;$State=$State;$ZipCode=$ZipCode"]]&gt;&lt;/attr&gt;
  &lt;/Node&gt;

 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jun 8, 2007
 *
 */
public class MemSeqNoIncrementer extends Node {

	
	private static final String XML_MEMSEQNO_FIELD_ATTRIBUTE = "memseqnoField";
	private static final String XML_NOTNULL_KEY_ATTRIBUTE = "notnullKey";
	private static final String XML_MAPPING_ATTRIBUTE = "mapping";

	public final static String COMPONENT_TYPE = "MEMSEQNO_INCREMENT";

	private final static int MAPPING_PORT = 0;
	private final static int COPY_PORT = 1;
	private final static int READ_FROM_PORT = 0;
	
	private String[] keys;
	private String memseqno;
	private String mapping;

	private int[] keyField;
	private int incrementIndex;
	private int[] fieldMapping;
	private Object[] constantMapping;
	
	/**
	 * @param id
	 */
	public MemSeqNoIncrementer(String id, String[] notnulls, String memseqno, String mapping) {
		super(id);
		this.keys = notnulls;
		this.memseqno = memseqno;
		this.mapping = mapping;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		DataRecordMetadata inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
		DataRecordMetadata outMetadata = getOutputPort(MAPPING_PORT).getMetadata();

		//create not null key
		RecordKey rKey = new RecordKey(keys, inMetadata);
		rKey.init();
		keyField = rKey.getKeyFields();
		
		//get index of field to increment
		incrementIndex = inMetadata.getFieldPosition(memseqno);
		
		//prepare fields for mapping and constants
		AggregateMappingParser mappingParser;
		try {
			mappingParser = new AggregateMappingParser(mapping, 
					new RecordKey((String[])inMetadata.getFieldNames().keySet().toArray(new String[0]),inMetadata), null, 
					inMetadata, outMetadata);
		}catch(AggregationException e){
			ComponentNotReadyException ex = new ComponentNotReadyException(e);
			ex.setAttributeName(XML_MAPPING_ATTRIBUTE);
			throw ex;
		}
		
		fieldMapping = new int[outMetadata.getNumFields()];
		Arrays.fill(fieldMapping, -1);
		List<FieldMapping> fMapping = mappingParser.getFieldMapping();
		for (FieldMapping mapping : fMapping) {
			fieldMapping[outMetadata.getFieldPosition(mapping.getOutputField())] = 
				inMetadata.getFieldPosition(mapping.getInputField());
		}
		
		constantMapping = new Object[outMetadata.getNumFields()];
		List<ConstantMapping> cMapping = mappingParser.getConstantMapping();
		for (ConstantMapping mapping : cMapping) {
			constantMapping[outMetadata.getFieldPosition(mapping.getOutputField())] = 
				mapping.getValue();
		}
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		DataRecordMetadata inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
		
        checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 2, 2);
        checkMetadata(status, inMetadata, getOutputPort(COPY_PORT).getMetadata());

        ConfigurationProblem problem;
		try {
			RecordKey rKey = new RecordKey(keys, inMetadata);
			rKey.init();
		} catch (Exception e) {
    		problem = new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, 
    				Priority.NORMAL, XML_NOTNULL_KEY_ATTRIBUTE); 
    		status.add(problem);
		}		
		
		if (inMetadata.getFieldPosition(memseqno) == -1) {
    		problem = new ConfigurationProblem(StringUtils.quote(memseqno) + " field does not exist in input metadata!!!", 
    				Severity.ERROR, this, Priority.NORMAL, XML_MEMSEQNO_FIELD_ATTRIBUTE); 
    		status.add(problem);
		}
		
		AggregateMappingParser mappingParser;
		try {
			mappingParser = new AggregateMappingParser(mapping, 
					new RecordKey((String[])inMetadata.getFieldNames().keySet().toArray(new String[0]),inMetadata), null, 
					inMetadata, getOutputPort(MAPPING_PORT).getMetadata());
		}catch(AggregationException e){
	   		problem = new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, 
    				Priority.NORMAL, XML_MAPPING_ATTRIBUTE); 
    		status.add(problem);
		}

		return status;
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_NOTNULL_KEY_ATTRIBUTE, StringUtils.stringArraytoString(keys,';'));
		xmlElement.setAttribute(XML_MEMSEQNO_FIELD_ATTRIBUTE, memseqno);
		xmlElement.setAttribute(XML_MAPPING_ATTRIBUTE, mapping);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort copyPort = getOutputPort(COPY_PORT);
		OutputPort mappingPort = getOutputPort(MAPPING_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		Numeric memseqnoField = (Numeric)inRecord.getField(incrementIndex);
		DataRecord outRecord = new DataRecord(mappingPort.getMetadata());
		outRecord.init();
		outRecord.reset();

		while ((inRecord = inPort.readRecord(inRecord)) != null && runIt){
			if (!allNull(inRecord)) {
				memseqnoField.setValue(memseqnoField.getInt() + 1);
				mappingPort.writeRecord(reformat(inRecord, outRecord));
			}
			copyPort.writeRecord(inRecord);
			SynchronizeUtils.cloverYield();
		}
		
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/**
	 * This method checks if all fields set as key have null value
	 * 
	 * @param inRecord record to check
	 * @return true if all key fields have null value, false in another case
	 */
	private boolean allNull(DataRecord inRecord){
		for (int i = 0; i < keyField.length; i++) {
			if (!inRecord.getField(keyField[i]).isNull()) return false;
		}
		return true;
	}
	
	/**
	 * Fills output record by mapping from input record and by constants due to values of
	 * 	fieldMapping and constantMapping
	 * 
	 * @param inRecord
	 * @param outRecord
	 * @return
	 */
	private DataRecord reformat(DataRecord inRecord, DataRecord outRecord){
		for (int i=0; i<outRecord.getNumFields(); i++){
			if (fieldMapping[i] != -1) {
				outRecord.getField(i).setValue(inRecord.getField(fieldMapping[i]));
			}else if (constantMapping[i] != null) {
				outRecord.getField(i).setValue(constantMapping[i]);
			}
		}
		return outRecord;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		try{
			return new MemSeqNoIncrementer(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_NOTNULL_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString(XML_MEMSEQNO_FIELD_ATTRIBUTE),
					xattribs.getString(XML_MAPPING_ATTRIBUTE));
		}catch(AttributeNotFoundException e){
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + e.getMessage(),e);
		}
	}	
}
