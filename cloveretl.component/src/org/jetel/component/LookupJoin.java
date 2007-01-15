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

import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>LookupJoin Component</h3>
 * <!-- Joins records from input port and lookup table based on specified key.
 * The flow on port 0 is the driver, record from lokup table is the slave. For
 * every record from driver flow, corresponding record from slave flow is looked
 * up (if it exists). -->
 * 
 * <table border="1">
 * 
 * <th> Component: </th>
 * <tr>
 * <td>
 * <h4><i>Name:</i> </h4>
 * </td>
 * <td>LookupJoin</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i> </h4>
 * </td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i> </h4>
 * </td>
 * <td> Joins records on input port and from lookup table. It expects that on
 * port [0], there is a driver and from lookup table is a slave<br>
 * For each driver record, slave record is looked up in lookup table. Pair of
 * driver and slave records is sent to transformation class.<br>
 * The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 * It skips driver records for which there is no corresponding slave - unless
 * outer join (leftOuterJoin option) is specified, when only driver record is
 * passed to transform method. </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i> </h4>
 * </td>
 * <td> [0] - primary records<br>
 * </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i> </h4>
 * </td>
 * <td> [0] - one output port </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i> </h4>
 * </td>
 * <td></td>
 * </tr>
 * </table> <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"LOOKUP_JOIN"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * </tr>
 * <tr>
 * <td><b>joinKey</b></td>
 * <td>field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 * </td>
 * </tr>
 * <tr>
 * <td><b>transform</b></td>
 * <td>contains definition of transformation in internal clover format or as
 * java code</td>
 * <tr>
 * <td><b>transformClass</b><br>
 * <i>optional</i></td>
 * <td>name of the class to be used for transforming joined data<br>
 * If no class name is specified then it is expected that the transformation
 * Java source code is embedded in XML
 * <tr>
 * <td><b>lookupTable</b></td>
 * <td>name of lookup table where are slave records</td>
 * <tr>
 * <td><b>leftOuterJoin</b><i>optional</i>
 * <td>true/false<I> default: FALSE</I> See description.</td>
 * <tr>
 * <td><b>freeLookupTable</b><i>optional</i>
 * <td>true/false<I> default: FALSE</I> idicates if close lookup table after
 * finishing execute() method. All records, which are stored only in memory will
 * be lost.</td>
 * </table>
 * <h4>Example:</h4>
 * 
 * <pre>
 *   &lt;Node id=&quot;JOIN&quot; type=&quot;LOOKUP_JOIN&quot;&gt;
 *   &lt;attr name=&quot;lookupTable&quot;&gt;LookupTable0&lt;/attr&gt;
 *   &lt;attr name=&quot;joinKey&quot;&gt;EmployeeID&lt;/attr&gt;
 *   &lt;attr name=&quot;transform&quot;&gt;
 *   import org.jetel.component.DataRecordTransform;
 *   import org.jetel.data.DataRecord;
 *   import org.jetel.data.RecordKey;
 *   import org.jetel.data.lookup.LookupTable;
 *   import org.jetel.exception.JetelException;
 *   import org.jetel.graph.TransformationGraph;
 *   
 *   public class reformatTest extends DataRecordTransform{
 *   
 *   	public boolean transform(DataRecord[] source, DataRecord[] target){
 *   	        
 *   
 *   		if (source[1]==null) return false; // skip this one
 *   
 *   		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *     		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *   		target[0].getField(2).setValue(source[0].getField(2).getValue());
 *   		target[0].getField(3).setValue(source[1].getField(0).getValue().toString());
 *   		target[0].getField(4).setValue(source[1].getField(1).getValue());
 *   
 *   		return true;
 *   	}
 *   }
 *   &lt;/attr&gt;
 *   &lt;/Node&gt;
 * </pre>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting
 *         s.r.o. www.javlinconsulting.cz
 * 
 * @since Dec 11, 2006
 * 
 */
public class LookupJoin extends Node {

	private static final String XML_LOOKUP_TABLE_ATTRIBUTE = "lookupTable";

	private static final String XML_FREE_LOOKUP_TABLE_ATTRIBUTE = "freeLookupTable";

	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";

	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";

	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";

	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";

	public final static String COMPONENT_TYPE = "LOOKUP_JOIN";

	private final static int WRITE_TO_PORT = 0;

	private final static int READ_FROM_PORT = 0;

	private String transformClassName = null;

	private RecordTransform transformation = null;

	private String transformSource = null;

	private String lookupTableName;

	private boolean freeLookupTable = false;

	private String[] joinKey;

	private boolean leftOuterJoin = false;

	private Properties transformationParameters = null;

	private LookupTable lookupTable;

	private RecordKey recordKey;

	private DataRecordMetadata lookupMetadata;

	static Log logger = LogFactory.getLog(Reformat.class);

	/**
	 * @param id component identification
	 * @param lookupTableName
	 * @param joinKey
	 * @param transform
	 * @param transformClass
	 */
	public LookupJoin(String id, String lookupTableName, String[] joinKey,
			String transform, String transformClass) {
		super(id);
		this.lookupTableName = lookupTableName;
		this.joinKey = joinKey;
		this.transformClassName = transformClass;
		this.transformSource = transform;
	}

	public LookupJoin(String id, String lookupTableName, String[] joinKey,
			RecordTransform transform) {
		this(id, lookupTableName, joinKey, null, null);
		this.transformation = transform;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		// initialize in and out records
		InputPort inPort = getInputPort(WRITE_TO_PORT);
		DataRecord[] outRecord = { new DataRecord(getOutputPort(READ_FROM_PORT)
				.getMetadata()) };
		outRecord[0].init();
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord[] inRecords = new DataRecord[] { inRecord, null };
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
			if (inRecord != null) {
				// find slave record in database
				inRecords[1] = lookupTable.get(inRecord);
				do {
					if ((inRecords[1] != null || leftOuterJoin)
							&& transformation.transform(inRecords, outRecord)) {
						writeRecord(WRITE_TO_PORT, outRecord[0]);
					}
					// get next record from database with the same key
					inRecords[1] = lookupTable.getNext();
				} while (inRecords[1] != null);
			}
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
		super.free();
		if (freeLookupTable) {
			lookupTable.free();
		}
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 1, 1);

        try {
        	
    		lookupTable = getGraph().getLookupTable(lookupTableName);
    		if (lookupTable == null) {
    			throw new ComponentNotReadyException("Lookup table \""
    					+ lookupTableName + "\" not found.");
    		}
    		if (!lookupTable.isInitialized()) {
    			lookupTable.init();
    		}
    		lookupMetadata = lookupTable.getMetadata();
    		DataRecordMetadata inMetadata[] = {
    				getInputPort(READ_FROM_PORT).getMetadata(), lookupMetadata };
    		DataRecordMetadata outMetadata[] = { getOutputPort(WRITE_TO_PORT)
    				.getMetadata() };
    		try {
    			recordKey = new RecordKey(joinKey, inMetadata[0]);
    			recordKey.init();
    			lookupTable.setLookupKey(recordKey);
    		} catch (Exception e) {
    			throw new ComponentNotReadyException(this, e);
    		}
        	
        	
    		if (freeLookupTable) {
    			lookupTable.free();
    		}
//            init();
//            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		// Initializing lookup table
		lookupTable = getGraph().getLookupTable(lookupTableName);
		if (lookupTable == null) {
			throw new ComponentNotReadyException("Lookup table \""
					+ lookupTableName + "\" not found.");
		}
		if (!lookupTable.isInitialized()) {
			lookupTable.init();
		}
		lookupMetadata = lookupTable.getMetadata();
		DataRecordMetadata inMetadata[] = {
				getInputPort(READ_FROM_PORT).getMetadata(), lookupMetadata };
		DataRecordMetadata outMetadata[] = { getOutputPort(WRITE_TO_PORT)
				.getMetadata() };
		try {
			recordKey = new RecordKey(joinKey, inMetadata[0]);
			recordKey.init();
			lookupTable.setLookupKey(recordKey);
			if (transformation != null){
				transformation.init(transformationParameters, inMetadata, outMetadata);
			}else{
				transformation = RecordTransformFactory.createTransform(
						transformSource, transformClassName, this, inMetadata,
						outMetadata, transformationParameters, this.getClass().getClassLoader());
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement)
			throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(
				xmlElement, graph);
		LookupJoin join;
		String[] joinKey;
		// get necessary parameters
		try {
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(
					Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

			join = new LookupJoin(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_LOOKUP_TABLE_ATTRIBUTE), joinKey,
					xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), xattribs
							.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null));
			join.setTransformationParameters(xattribs
							.attributes2Properties(new String[] { XML_TRANSFORM_CLASS_ATTRIBUTE }));
			if (xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)) {
				join.setLeftOuterJoin(xattribs
						.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE));
			}
			join.setFreeLookupTable(xattribs.getBoolean(
					XML_FREE_LOOKUP_TABLE_ATTRIBUTE, false));
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ")
					+ ":" + ex.getMessage(), ex);
		}

		return join;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_LOOKUP_TABLE_ATTRIBUTE, lookupTableName);
		xmlElement.setAttribute(XML_FREE_LOOKUP_TABLE_ATTRIBUTE, String
				.valueOf(freeLookupTable));
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORM_CLASS_ATTRIBUTE,
					transformClassName);
		}

		if (transformSource != null) {
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE, transformSource);
		}
		xmlElement.setAttribute(XML_JOIN_KEY_ATTRIBUTE, StringUtils
				.stringArraytoString(joinKey, ';'));

		xmlElement.setAttribute(XML_LEFTOUTERJOIN_ATTRIBUTE, String
				.valueOf(leftOuterJoin));

		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String) propertyAtts.nextElement();
			xmlElement.setAttribute(attName, transformationParameters
					.getProperty(attName));
		}
	}

	/**
	 * @param transformationParameters
	 *            The transformationParameters to set.
	 */
	public void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}

	private void setLeftOuterJoin(boolean leftOuterJoin) {
		this.leftOuterJoin = leftOuterJoin;
	}

	private void setFreeLookupTable(boolean freeLookupTable) {
		this.freeLookupTable = freeLookupTable;
	}

}
