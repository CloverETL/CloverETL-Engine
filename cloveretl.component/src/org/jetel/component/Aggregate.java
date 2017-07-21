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

import java.nio.charset.Charset;
import java.util.Iterator;

import org.jetel.component.aggregate.AggregateProcessor;
import org.jetel.component.aggregate.AggregationException;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <!-- Aggregate functions ara applied on input data flow base on specified key.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Aggregate</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Aggregate functions are applied on input data flow base on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  Data flow can be sorted or not.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"AGGREGATE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>aggregateKey</b></td><td>aggregation key, field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>mapping</b></td><td>aggregation function mappings separated by :;|  {colon, semicolon, pipe}. 
 *  Each mapping can be in the form of $output_field=$input_field or $output_field=function($input_field).
 *  The first form copies fields from input to output without applying any aggregation function 
 *  (the input field must be part of the key). The second form applies an aggregation function on all values
 *  of the input field (within an aggregation group implied by the aggregation key) and copies the result
 *  to the output field. Some function don't require an input field
 *  as a parameter (then the function mapping is in the form output_field=function(). Available functions 
 *  are count, countunique, min, max, sum, avg, stdev, CRC32, MD5, first, last, firstnonnull, lastnonnull, 
 *  median, modus. The 
 *  function names are case insensitive</td>
 *  <tr><td><b>sorted</b></td><td>if input data flow is sorted (true)</td>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is FALSE.</td></tr>
 *  <tr><td><b>charset</b></td><td>character encoding of the input data stream for CRC32 and MD5 functions (if not specified, then value from defaultProperties DataFormatter.DEFAULT_CHARSET_ENCODER is used)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="AGGREGATE_NODE" type="AGGREGATE" aggregateKey="FirstName" aggregateFunctions="count(); min(Age); avg(Salery); min(HireDate)" sorted="false" /&gt;</pre>
 *
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @since       June 27, 2005
 */
public class Aggregate extends Node {
	/** Name of the component. */
	public final static String COMPONENT_TYPE = "AGGREGATE";
	
	// required attributes
	private static final String XML_AGGREGATE_KEY_ATTRIBUTE = "aggregateKey";
	private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private static final String XML_OLD_MAPPING_ATTRIBUTE = "aggregateFunctions";
	private static final String XML_SORTED_ATTRIBUTE = "sorted";
	// optional attributes
    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";

	// used ports
	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private static int UNDETECTED_DIRECTION = Integer.MAX_VALUE;

	private String[] aggregateKeys;
	private String newMapping;
	private String oldMapping;
	private boolean sorted;
	
	private boolean equalNULLs;
	private String charset;

	private AggregateProcessor processor;
	private RecordKey recordKey;

	/**
	 * 
	 * Allocates a new <tt>Aggregate</tt> object.
	 *
	 * @param id unique ID of the component.
	 * @param aggregateKeys aggregation keys.
	 * @param mapping aggregation function mapping.
	 * @param sorted specifies if the input is sorted.
	 * @param oldMapping set to <tt>true</tt> if the function mapping is in the old format.
	 */
	public Aggregate(String id, String[] aggregateKeys, String mapping, boolean sorted, String oldMapping) {
		super(id);
		
		this.aggregateKeys = aggregateKeys;
		this.newMapping = mapping;
		this.sorted = sorted;
		this.oldMapping = oldMapping;
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		processor.reset();
    	}
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		if (sorted) {
			boolean firstLoop = true;
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = DataRecordFactory.newRecord(inPort.getMetadata());
			DataRecord previousRecord = DataRecordFactory.newRecord(inPort.getMetadata());
			DataRecord tempRecord;
			DataRecord outRecord = DataRecordFactory.newRecord(outPort.getMetadata());

			currentRecord.init();
			previousRecord.init();
			outRecord.init();
			outRecord.reset();

			int sortDirection = UNDETECTED_DIRECTION;
			int currentSortDirection = UNDETECTED_DIRECTION;
			int recordCount = 0;

			while (currentRecord != null && runIt) {
				currentRecord = inPort.readRecord(currentRecord);
				recordCount++;
				if (!firstLoop) {
					if (currentRecord == null || 
							((currentSortDirection = recordKey.compare(currentRecord, previousRecord)) != 0
									&& /* fix of CL-1753 */ (!recordKey.isEqualNULLs() || !recordKey.isComparedNulls()))) { 
						// next group founded
						
						if (!recordKey.isComparedNulls()) {
							// check sort direction whether it is still the same
							if (sortDirection == UNDETECTED_DIRECTION) {
								sortDirection = currentSortDirection;
							} else if (currentRecord != null && sortDirection != currentSortDirection) {
								StringBuilder sb = new StringBuilder();
								DataField field = recordKey.getFieldViolatingEquals(currentRecord, previousRecord);
								sb.append("Data on input port #");
								sb.append(READ_FROM_PORT);
								sb.append(" is not sorted by aggregation key(s); error caused by field #");
								sb.append(recordCount);
								sb.append(" \"");
								sb.append(field.getMetadata().getName());
								sb.append("\", value \"");
								sb.append(field.getValue());
								sb.append("\" being out of order. Hints for fix: sort input; configure aggregation " +
										"to expect not sorted input or review aggregation key.");
								throw new JetelException(sb.toString());
							}
						}
						
						processor.getCurrentSortedAggregationOutput(outRecord);
						writeRecordBroadcast(outRecord);
					}
				} else {
					firstLoop = false;
				}
				// switch previous and current record
				if (currentRecord != null) {
					processor.addRecord(currentRecord);

					tempRecord = previousRecord;
					previousRecord = currentRecord;
					currentRecord = tempRecord;
				}
			}
		} else { // sorted == false
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = DataRecordFactory.newRecord(inPort.getMetadata());
			DataRecord outRecord = DataRecordFactory.newRecord(outPort.getMetadata());

			currentRecord.init();
			outRecord.init();
			outRecord.reset();

			// read all data from input port to aggregateRecord
			while ((currentRecord = inPort.readRecord(currentRecord)) != null && runIt) {
				processor.addRecord(currentRecord);
			}
			
			for (Iterator<DataRecord> results = processor.getUnsortedAggregationOutput(outRecord);
				results.hasNext(); ) {
				writeRecordBroadcast(results.next());
			}
		}
		
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		recordKey = new RecordKey(aggregateKeys, getInputPort(READ_FROM_PORT).getMetadata());
		recordKey.init();
		// specify whether two fields with NULL value indicator set are considered equal
		recordKey.setEqualNULLs(equalNULLs);
		
		String mapping;
		boolean isOldMapping;
		if (newMapping != null) {
			mapping = newMapping;
			isOldMapping = false;
		}
		else if (oldMapping != null) {
			mapping = oldMapping;
			isOldMapping = true;
		}
		else {
			throw new ComponentNotReadyException(XML_MAPPING_ATTRIBUTE + " attribute not specified");
		}
		
		try {
			processor = new AggregateProcessor(mapping, isOldMapping, recordKey, sorted, 
					getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(WRITE_TO_PORT).getMetadata(),
					charset);
		} catch (AggregationException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 * 
	 * @param graph
	 * @param xmlElement
	 * @return component loaded from XML.
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException 
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		String[] aggregateKey = new String[0];
		String newMapping = null;
		String oldMapping = null;
        boolean sorted = true;

    	//read aggregate key attribute
        if(xattribs.exists(XML_AGGREGATE_KEY_ATTRIBUTE)) {
            aggregateKey = xattribs.getString(XML_AGGREGATE_KEY_ATTRIBUTE).split(
            		Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);                
        } 
        
        //read mapping attribute
        if (xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
        	newMapping = xattribs.getString(XML_MAPPING_ATTRIBUTE);
        }

        //read old mapping attribute
        if (xattribs.exists(XML_OLD_MAPPING_ATTRIBUTE)) {
        	oldMapping = xattribs.getString(XML_OLD_MAPPING_ATTRIBUTE);
        }
        
        if (newMapping == null && oldMapping == null) {
        	throw new XMLConfigurationException("Aggregation mapping must be provided in the " +
        			XML_MAPPING_ATTRIBUTE + " or " + XML_OLD_MAPPING_ATTRIBUTE + " attribute");
        }
        
        //read sorted attribute
        if(xattribs.exists(XML_SORTED_ATTRIBUTE)) {
            sorted = xattribs.getBoolean(XML_SORTED_ATTRIBUTE);                
        }
        //make an instance of the component
	    Aggregate aggregate = new Aggregate(xattribs.getString("id"), 
	    		aggregateKey,
				newMapping,
                sorted,
                oldMapping);
		
		// read optional attributes
		if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
			aggregate.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
		}
        if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
        	aggregate.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
        }
        
		return aggregate;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        checkMetadata(status, getOutMetadata());
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }
        
        if (newMapping == null && oldMapping == null) {
        	status.add(new ConfigurationProblem("Mapping not specified", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        	return status;
        }
        
        if (newMapping != null && oldMapping != null) {
        	status.add(new ConfigurationProblem(
        			"Both " + XML_MAPPING_ATTRIBUTE + " and " + XML_OLD_MAPPING_ATTRIBUTE + " attributes specified",
        			ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        	return status;
        }

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

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/**
	 * @return the equalNULLs
	 */
	public boolean isEqualNULLs() {
		return equalNULLs;
	}

	/**
	 * @param equalNULLs the equalNULLs to set
	 */
	public void setEqualNULLs(boolean equalNULLs) {
		this.equalNULLs = equalNULLs;
	}

	/**
	 * @return the charset
	 */
	public String getCharset() {
		return charset;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
	
}
