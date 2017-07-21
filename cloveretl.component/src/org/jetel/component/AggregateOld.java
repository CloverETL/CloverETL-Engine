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

import java.util.Iterator;

import org.jetel.component.aggregate.AggregateFunctionOld;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Aggregate Component</h3>
 *
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
 *  Data flow can be sorted or not. On this component you cannot set any transformation function
 *  to map aggregation results on the output metadata. Output metadata has to correspond accurately
 *  to the settings of aggregate component. Key of aggregation is mapped first and then follow
 *  all aggregate function results.</td></tr>
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
 *  <tr><td><b>aggregateKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>aggregateFunction</b></td><td>aggregate functions separated by :;|  {colon, semicolon, pipe} available functions are count, min, max, sum, avg, stdev, CRC32, MD5, FIRST, LAST</td>
 *  <tr><td><b>sorted</b></td><td>if input data flow is sorted (true)</td>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is FALSE.</td></tr>
 *  <tr><td><b>charset</b></td><td>character encoding of the input data stream for CRC32 and MD5 functions (if not specified, then value from defaultProperties DataFormatter.DEFAULT_CHARSET_ENCODER is used)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="AGGREGATE_NODE" type="AGGREGATE" aggregateKey="FirstName" aggregateFunctions="count(); min(Age); avg(Salery); min(HireDate)" sorted="false" /&gt;</pre>
 *
 * @author      Martin Zatopek, Javlin Consulting s.r.o. (www.javlinconsulting.cz)
 * @since       June 27, 2005
 */
public class AggregateOld extends Node {

    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";

	public final static String COMPONENT_TYPE = "AGGREGATE";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private boolean sorted;
	private String[] aggregateKeys;
	private String aggregateFunctionStr;
	private RecordKey recordKey;
	private AggregateFunctionOld aggregateFunction;
	private boolean equalNULLs;
	private String charset;

	/**
	 *Constructor for the Aggregate object
	 *
	 * @param  id         Description of the Parameter
	 * @param  dedupKeys  Description of the Parameter
	 * @param  keepFirst  Description of the Parameter
	 */
	public AggregateOld(String id, String[] aggregateKeys, String aggregateFunctions, boolean sorted) {
		super(id);
		this.sorted = sorted;
		this.aggregateKeys = aggregateKeys;
		this.aggregateFunctionStr = aggregateFunctions;
	}

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

			while (currentRecord != null && runIt) {
				currentRecord = inPort.readRecord(currentRecord);
				if (!firstLoop) {
					if (currentRecord == null
							|| recordKey.compare(currentRecord, previousRecord) != 0) { // next group founded
						writeRecordBroadcast(aggregateFunction
								.getRecordForGroup(previousRecord, outRecord));
					}
				} else {
					firstLoop = false;
				}
				// switch previous and current record
				if (currentRecord != null) {
					aggregateFunction.addSortedRecord(currentRecord);

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

			// read all data from input port to aggregateRecord
			while ((currentRecord = inPort.readRecord(currentRecord)) != null && runIt) {
				aggregateFunction.addUnsortedRecord(currentRecord);
			}
			// write agragated data to outputport from aggregateRecord
			for (Iterator i = aggregateFunction.iterator(outRecord); i.hasNext();) {
				writeRecordBroadcast((DataRecord) i.next());
			}
		}

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Initialize method of aggregate component
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		recordKey = new RecordKey(aggregateKeys, getInputPort(READ_FROM_PORT).getMetadata());
		// for AGGREGATE component, specify whether two fields with NULL value indicator set
		// are considered equal
		recordKey.setEqualNULLs(equalNULLs);

		aggregateFunction = new AggregateFunctionOld(aggregateFunctionStr, getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(WRITE_TO_PORT).getMetadata(), recordKey, sorted, charset);
		aggregateFunction.init();
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		String[] aggregateKey = new String[0];
        boolean sorted = true;

        //read aggregate key attribute
        if(xattribs.exists("aggregateKey")) {
            aggregateKey = xattribs.getString("aggregateKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);                
        }
        //read sorted attribute
        if(xattribs.exists("sorted")) {
            sorted = xattribs.getString("sorted").matches("^[Tt].*");                
        }
        //make instance of aggregate component
	    AggregateOld agg;
		agg = new AggregateOld(xattribs.getString("id"),
				aggregateKey,
				xattribs.getString("aggregateFunctions"),
                sorted);
		if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
		    agg.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
		}
        if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
            agg.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
        }
		return agg;
	}


	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }

        try {
            init();
        } catch (ComponentNotReadyException e) {
			status.addError(this, null, e);
        } finally {
        	free();
        }
        
        return status;
    }
	
	public void setEqualNULLs(boolean equal){
	    this.equalNULLs=equal;
	}

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

}

