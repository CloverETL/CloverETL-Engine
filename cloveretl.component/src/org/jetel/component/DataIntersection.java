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
package org.jetel.component;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Data Intersection Component</h3> <!-- Finds intersection of
 * flow A (in-port0) and B (in-port1) based on specified key. Both inputs
 * must be sorted according to specified key. DataRecords only in flow A
 * are sent out through out-port0. DataRecords in both A & B are sent to
 * specified transformation function and the result is sent through out-port1.
 * DataRecords present only in flow B are sent through out-port2.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>DataIntersection</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 * Finds intersection of data flows (sets) <b>A (in-port0)</b> and <b>B (in-port1)</b> 
 * based on specified key. Both inputs <u><b>must be sorted</b></u> according to specified key. DataRecords only in flow <b>A</b>
 * are sent out through <b>out-port[0]</b>.
 * DataRecords in both <b>A&amp;B</b> are sent to specified <b>transformation</b> function and the result is 
 * sent through <b>out-port[1]</b>.
 * DataRecords present only in flow <b>B</b> are sent through <b>out-port[2]</b>.<br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - records from set A - <i>sorted according to specified key</i><br>
 *	  [1] - records from set B - <i>sorted according to specified key</i><br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - records only in set A<br>
 * 		  [1] - records in set A&amp;B<br>
 * 		  [2] - records only in set B	
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"DATA_INTERSECTION"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>can be used to specify different key field names for records on slave input; field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *      to search for class to be used for transforming data specified in <tt>transformClass<tt> parameter.</td></tr>
 *    <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td></tr>
 *    <tr><td><b>transform</b></td><td>contains definition of transformationas java source, in internal clover format or in Transformation Language </td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is TRUE.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="EmployeeID"&gt;
 *&lt;attr name="javaSource"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class intersectionTest extends DataRecordTransform{
 *
 *	public boolean transform(DataRecord[] source, DataRecord[] target){
 *		
 *		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *		target[0].getField(2).setValue(source[1].getField(2).getValue());
 *		return true;
 *	}
 *}
 *&lt;/attr&gt;
 *&lt;/Node&gt;</pre>
 * @author      dpavlis
 * @since       April 29, 2005
 * @revision    $Revision$
 * @created     29. April 2005
 */
public class DataIntersection extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_INTERSECTION";

	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
    private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";

	private final static int WRITE_TO_PORT_A = 0;
	private final static int WRITE_TO_PORT_A_B = 1;
	private final static int WRITE_TO_PORT_B = 2;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;

	private String transformClassName;
    private String transformSource = null;
	private String transformURL = null;
	private String charset = null;

	private RecordTransform transformation = null;

	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;

	private RecordKey recordKeys[];
    private boolean equalNULLs = true;

	// for passing data records into transform function
	private DataRecord[] inRecords = new DataRecord[2];
	private DataRecord[] outRecords=new DataRecord[2];

	private Properties transformationParameters;
	
	static Log logger = LogFactory.getLog(DataIntersection.class);
	
	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param  id              id of component
	 * @param  joinKeys        field names composing key
	 * @param  transformClass  class (name) to be used for transforming data
	 */
	public DataIntersection(String id, String[] joinKeys, String transform,
			String transformClass, String transformURL) {
		super(id);
		this.joinKeys = joinKeys;
		this.transformClassName = transformClass;
		this.transformSource = transform;
		this.transformURL = transformURL;
	}

	public DataIntersection(String id, String[] joinKeys, DataRecordTransform transform) {
		this(id, joinKeys, null, null, null);
		this.transformation = transform;
	}

	/**
	 *  Sets specific key (string) for slave records<br>
	 *  Can be used if slave record has different names
	 *  for fields composing the key
	 *
	 * @param  slaveKeys  The new slaveOverrideKey value
	 */
	public void setSlaveOverrideKey(String[] slaveKeys) {
		this.slaveOverrideKeys = slaveKeys;
	}


	/**
	 *  Finds corresponding slave record for current driver (if there is some)
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  slavePort                 Description of the Parameter
	 * @param  key                       Description of the Parameter
	 * @return                           The correspondingRecord value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
//	private int getCorrespondingRecord(DataRecord driver, DataRecord slave, InputPort slavePort, RecordKey key[]){
//
//		if (slave != null) {
//			return key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave);
//		}
//		return -1;
//	}


	/**
	 *  Outputs all combinations of current driver record and all slaves with the
	 *  same key
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  out                       Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 * @throws TransformException 
	 */
	private final boolean flushCombinations(DataRecord driver, DataRecord slave, DataRecord out, OutputPort port)
	throws IOException, InterruptedException, TransformException {
	    inRecords[0] = driver;
	    inRecords[1] = slave;
	    outRecords[0]= out;
	    
	    if (!transformation.transform(inRecords, outRecords)) {
	        logger.warn(transformation.getMessage());
	        return false;
	    }
	    port.writeRecord(out);
	    return true;
}


	/**
	 *  If there is no corresponding slave record and is defined left outer join, then
	 *  output driver only.
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private final boolean flushDriverOnly(DataRecord driver, OutputPort port) 
			throws IOException,InterruptedException{
		port.writeRecord(driver);
		return true;
	}

	/**
	 *  If there is no corresponding driver record and is defined full outer join, then
	 *  output slave only.
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private final boolean flushSlaveOnly(DataRecord slave, OutputPort port)
				throws IOException,InterruptedException{
	    port.writeRecord(slave);
	    return true;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  metadata  Description of the Parameter
	 * @param  count     Description of the Parameter
	 * @return           Description of the Return Value
	 */
//	private DataRecord[] allocateRecords(DataRecordMetadata metadata, int count) {
//		DataRecord[] data = new DataRecord[count];
//
//		for (int i = 0; i < count; i++) {
//			data[i] = new DataRecord(metadata);
//			data[i].init();
//		}
//		return data;
//	}
	
	@Override
	public Result execute() throws Exception {
		// get all ports involved
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort outPortA = getOutputPort(WRITE_TO_PORT_A);
		OutputPort outPortB = getOutputPort(WRITE_TO_PORT_B);
		OutputPort outPortAB = getOutputPort(WRITE_TO_PORT_A_B);

		// initialize input records driver & slave
		DataRecord driverRecord = new DataRecord(driverPort.getMetadata());
		driverRecord.init();
		DataRecord slaveRecord = new DataRecord(slavePort.getMetadata());
		slaveRecord.init();

		// initialize output record
		DataRecord outRecord = new DataRecord(outPortAB.getMetadata());
		outRecord.init();
		outRecord.reset();

		// first initial load of records
		driverRecord = driverPort.readRecord(driverRecord);
		slaveRecord = slavePort.readRecord(slaveRecord);
		// main processing loop
		while (runIt && driverRecord != null && slaveRecord != null) {
			switch (recordKeys[DRIVER_ON_PORT].compare(
					recordKeys[SLAVE_ON_PORT], driverRecord, slaveRecord)) {
			case -1:
				// driver lower
				// no corresponding slave
				flushDriverOnly(driverRecord, outPortA);
				driverRecord = driverPort.readRecord(driverRecord);
				break;
			case 0:
				// match - perform transformation
				flushCombinations(driverRecord, slaveRecord, outRecord,
						outPortAB);
				// load in new driver & slave
				driverRecord = driverPort.readRecord(driverRecord);
				slaveRecord = slavePort.readRecord(slaveRecord);
				break;
			case 1:
				// slave lover - no corresponding master
				flushSlaveOnly(slaveRecord, outPortB);
				slaveRecord = slavePort.readRecord(slaveRecord);
				break;
			}
		}
		// flush remaining driver records
		while (driverRecord != null) {
			flushDriverOnly(driverRecord, outPortA);
			driverRecord = driverPort.readRecord(driverRecord);
		}
		// flush remaining slave records
		while (slaveRecord != null) {
			flushSlaveOnly(slaveRecord, outPortB);
			slaveRecord = slavePort.readRecord(slaveRecord);
		}
		transformation.finished();
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
     }

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		if (slaveOverrideKeys == null) {
			slaveOverrideKeys = joinKeys;
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
		recordKeys[1] = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
		recordKeys[0].init();
		recordKeys[1].init();
		//specify whether two fields with NULL value indicator set
        // are considered equal
        recordKeys[0].setEqualNULLs(equalNULLs);
        recordKeys[1].setEqualNULLs(equalNULLs);

        // init transformation
        DataRecordMetadata[] inMetadata = (DataRecordMetadata[]) getInMetadata()
                .toArray(new DataRecordMetadata[0]);
        DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { getOutputPort(
                WRITE_TO_PORT_A_B).getMetadata() };
		//create instance of record transformation
        if (transformation != null){
        	transformation.init(transformationParameters, inMetadata, outMetadata);
        }else{
			transformation = RecordTransformFactory.createTransform(transformSource, transformClassName, 
					transformURL, charset, this, inMetadata, outMetadata, transformationParameters, 
					this.getClass().getClassLoader());
        }
	}


    /**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
    }
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (joinKeys != null) {
			StringBuffer buf = new StringBuffer(joinKeys[0]);
			for (int i=1; i< joinKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + joinKeys[i]); 
			}
			xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE,buf.toString());
		}
		
		if (slaveOverrideKeys!= null) {
			StringBuffer buf = new StringBuffer(slaveOverrideKeys[0]);
			for (int i=1; i< slaveOverrideKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + slaveOverrideKeys[i]); 
			}
			xmlElement.setAttribute(XML_SLAVEOVERRIDEKEY_ATTRIBUTE,buf.toString());
		}
		
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE,transformClassName);
		} 
		
		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}
		
		if (transformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORMURL_ATTRIBUTE, transformURL);
		}
		
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
        
		// equal NULL attribute
        xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULLs));
		
		if (transformationParameters != null) {
			Enumeration propertyAtts = transformationParameters.propertyNames();
			while (propertyAtts.hasMoreElements()) {
				String attName = (String)propertyAtts.nextElement();
				xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
			}
		}
		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		DataIntersection intersection;

		try{
			intersection = new DataIntersection(
                    xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_JOINKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
                    xattribs.getString(XML_TRANSFORMURL_ATTRIBUTE,null));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				intersection.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SLAVEOVERRIDEKEY_ATTRIBUTE)) {
				intersection.setSlaveOverrideKey(xattribs.getString(XML_SLAVEOVERRIDEKEY_ATTRIBUTE).
						split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			}
            if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
                intersection.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
            }

			intersection.setTransformationParameters(xattribs.attributes2Properties(
	                new String[]{XML_ID_ATTRIBUTE,XML_JOINKEY_ATTRIBUTE,
	                		XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE,
	                		XML_SLAVEOVERRIDEKEY_ATTRIBUTE,XML_EQUAL_NULL_ATTRIBUTE}));
			
			return intersection;
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        DataRecordMetadata driverMetadata = getInputPort(DRIVER_ON_PORT).getMetadata();
        DataRecordMetadata slaveMetadata = getInputPort(SLAVE_ON_PORT).getMetadata();
        //port number checking
        checkInputPorts(status, 2, 2);
        checkOutputPorts(status, 3, 3);
        //compiliance of input and output metadata checking
		checkMetadata(status, driverMetadata, getOutputPort(WRITE_TO_PORT_A).getMetadata());
		checkMetadata(status, slaveMetadata, getOutputPort(WRITE_TO_PORT_B).getMetadata());

		//join key checking
		if (slaveOverrideKeys == null) {
			slaveOverrideKeys = joinKeys;
		}
		recordKeys = new RecordKey[2];
		recordKeys[0] = new RecordKey(joinKeys, driverMetadata);
		recordKeys[1] = new RecordKey(slaveOverrideKeys, slaveMetadata);
		RecordKey.checkKeys(recordKeys[0], XML_JOINKEY_ATTRIBUTE, recordKeys[1], 
				XML_SLAVEOVERRIDEKEY_ATTRIBUTE, status, this);
        
        return status;
    }
	
	public String getType(){
		return COMPONENT_TYPE;
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

