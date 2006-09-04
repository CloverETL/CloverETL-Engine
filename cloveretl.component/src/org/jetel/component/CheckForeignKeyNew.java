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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.BadAttributeValueExpException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.SetVal;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DuplicateKeyMap;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;
//TODO:

/**
 *  <h3>HashJoin Component</h3> <!-- Joins two records from two different
 * input flows based on specified key. The flow on port 0 is the driver, the flow
 * on port 1 is the slave. First, all records from slave flow are read and stored in
 * hash table. Then for every record from driver flow, corresponding record from
 * slave flow is looked up (if it exists) -->
 *
 *<table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>HashJoin</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *        Joins records on input ports. It expects that on port [0], there is a
 *	driver and on port [1] is a slave<br>
 *	For each driver record, slave record is looked up in Hashtable which is created
 *	from all records on slave port.
 *	Pair of driver and slave records is sent to transformation class.<br>
 *	The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 *	It skips driver records for which there is no corresponding slave - unless outer
 *	join is specified, when only driver record is passed to <i>transform</i> method.<br>
 *  In this case be sure, that your transform code is prepared processe null input records. 
 *	Hash join does not require input data be sorted. But it spends some time at the beginning
 *	initializing hashtable of slave records.
 *	It is generally good idea to specify how many records are expected to be stored in hashtable, especially
 *	when you expect the number to be really great. It is better to specify slightly greater number to ensure
 *	that rehashing won't occure. For small record sets - up to 512 records, there is no need to specify the
 *	size.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - driver records<br>
 *	  [1] - slave records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - one output port
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"HASH_JOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming joined data specified in <tt>transformClass<tt> parameter.</td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td>
 *    <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *    <tr><td><b>leftOuterJoin</b><br><i>optional</i></td><td>true/false  See description of the HashJoin component.</td></tr>
 *    <tr><td><b>hashTableSize</b><br><i>optional</i></td><td>how many records are expected (roughly) to be in hashtable.</td></tr>
 *    <tr><td><b>slaveDuplicates</b><br><i>optional</i></td><td>true/false - allow records on slave port with duplicate keys. Default is false - multiple
 *    duplicate records are discarded - only the last one read from port stays.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *	  
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID" leftOuterJoin="false"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class reformatJoinTest extends DataRecordTransform{
 *
 *	public boolean transform(DataRecord[] source, DataRecord[] target){
 *		
 *		target[0].getField(0).setValue(source[0].getField(0).getValue());
 *		target[0].getField(1).setValue(source[0].getField(1).getValue());
 *		target[0].getField(2).setValue(source[0].getField(2).getValue());
 *		if (source[1]!=null){
 *			target[0].getField(3).setValue(source[1].getField(0).getValue());
 *			target[0].getField(4).setValue(source[1].getField(1).getValue());
 *		}
 *		return true;
 *	}
 *}
 *
 *&lt;/Node&gt;</pre>
 *	  
 * @author      dpavlis
 * @since       March 09, 2004
 * @revision    $Revision: 1212 $
 * @created     09. March 2004
 */
    public class CheckForeignKeyNew extends Node {
    
    	private static final String XML_HASHTABLESIZE_ATTRIBUTE = "hashTableSize";
        
        private static final String XML_FOREIGNKEY_ATTRIBUTE = "foreignKey";
        private static final String XML_PRIMARYKEY_ATTRIBUTE = "primaryKey";
        private static final String XML_DEFAULTFOREIGNKEY_ATTRIBUTE = "defaultForeignKey";
    
    	/**  Description of the Field */
        public final static String COMPONENT_TYPE = "CHECK_FOREIGN_KEY";
    
    	private final static int DEFAULT_HASH_TABLE_INITIAL_CAPACITY = 512;
    
    	private final static int WRITE_TO_PORT = 0; // not really used - we broadcast
        private final static int PRIMARY_ON_PORT = 0;
        private final static int FOREIGN_ON_PORT = 1;
    
    
    	private String[] primaryKeys;
    	private String[] foreignKeys;
        private String[] defaultForeignKeys;
        private DataRecord defaultRecord;
        
    	private RecordKey primaryKey;
    	private RecordKey foreignKey;
    
    	private Map hashMap;
    	private int hashTableInitialCapacity;
    	
    
    	static Log logger = LogFactory.getLog(CheckForeignKeyNew.class);
    
    	/**
    	 *Constructor for the HashJoin object
    	 *
    	 * @param  id              Description of the Parameter
    	 * @param  joinKeys        Description of the Parameter
    	 * @param  transformClass  Description of the Parameter
    	 * @param  leftOuterJoin   Description of the Parameter
    	 */
    	public CheckForeignKeyNew(String id, String[] primaryKeys, String[] foreignKeys, 
                String[] defaultForeignKeys) {
    		super(id);
    		this.primaryKeys = primaryKeys;
    		this.foreignKeys =foreignKeys;
    		this.defaultForeignKeys = defaultForeignKeys;
    		this.hashTableInitialCapacity = DEFAULT_HASH_TABLE_INITIAL_CAPACITY;
    	}
    
    
    
    	/**
    	 *  Sets the hashTableInitialCapacity attribute of the HashJoin object
    	 *
    	 * @param  capacity  The new hashTableInitialCapacity value
    	 */
    	public void setHashTableInitialCapacity(int capacity) {
    		if (capacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY) {
    			hashTableInitialCapacity = capacity;
    		}
    	}
    
    
    	/**
         * Description of the Method
         * 
         * @exception ComponentNotReadyException
         *                Description of the Exception
         */
        public void init() throws ComponentNotReadyException {
            // test that we have at least one input port and one output
            if (inPorts.size() < 2) {
                throw new ComponentNotReadyException(
                        "At least two input ports have to be defined!");
            } else if (outPorts.size() < 1) {
                throw new ComponentNotReadyException(
                        "At least one output port has to be defined!");
            }
          
            (primaryKey = new RecordKey(primaryKeys, getInputPort(PRIMARY_ON_PORT)
                    .getMetadata())).init();
            (foreignKey = new RecordKey(foreignKeys,
                    getInputPort(FOREIGN_ON_PORT).getMetadata())).init();
    
            // allocate HashMap
            try {
                hashMap = new HashMap(hashTableInitialCapacity);
            } catch (OutOfMemoryError ex) {
                logger.fatal(ex);
            } finally {
                if (hashMap == null) {
                    throw new ComponentNotReadyException(
                            "Can't allocate HashMap of size: "
                                    + hashTableInitialCapacity);
                }
            }
            // get record consisting of key-fields only
            defaultRecord = new DataRecord(primaryKey.generateKeyRecordMetadata());
            for(int i=0;i<defaultForeignKeys.length;i++){
                try{
                    defaultRecord.getField(i).fromString(defaultForeignKeys[i]);
                }catch(BadDataFormatException ex){
                    throw new ComponentNotReadyException(this,"error when initializing DefaultForeignKey - "+ex.getMessage(),ex);
                }catch(IndexOutOfBoundsException ex){
                    throw new ComponentNotReadyException(this,"invalid number of default values (\"defaultForeignKey\" parameter) when initializing DefaultRecord");
                }
            }
        }
    
    
       
    	/**
    	 *  Main processing method for the SimpleCopy object
    	 *
    	 * @since    April 4, 2002
    	 */
    	public void run() {
    		InputPort inPrimaryPort = getInputPort(PRIMARY_ON_PORT);
    		InputPort inForeignPort = getInputPort(FOREIGN_ON_PORT);
    		DataRecord primaryRecord;
    		DataRecord foreignRecord;
    		DataRecord storeRecord;
    
    		foreignRecord=new DataRecord(inForeignPort.getMetadata());
    		foreignRecord.init();
    		
    		// first read all records from SLAVE port
    		while (foreignRecord!=null && runIt) {
    			try {
    				if ((foreignRecord=inForeignPort.readRecord(foreignRecord)) != null) {
    				    storeRecord=foreignRecord.duplicate();
    					hashMap.put(new HashKey(foreignKey, storeRecord),
    							storeRecord);
    				} 
    				SynchronizeUtils.cloverYield();
    
    			} catch (IOException ex) {
    				resultMsg = ex.getMessage();
    				resultCode = Node.RESULT_ERROR;
    				closeAllOutputPorts();
    				return;
    			} catch (Exception ex) {
    				resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
    				resultCode = Node.RESULT_FATAL_ERROR;
    				return;
    			}
    		}
    		//XDEBUG START
    //		if (logger.isDebugEnabled()) {
    //			for (Iterator i = hashMap.values().iterator(); i.hasNext();) {
    //				logger.debug("> " + i.next());
    //			}
    //			logger.debug("***KEYS***");
    //			for (Iterator i = hashMap.keySet().iterator(); i.hasNext();) {
    //				logger.debug("> " + i.next());
    //			}
    //		}
    		//XDEBUG END
    
    		// now read all records from DRIVER port and try to look up corresponding
    		// record from SLAVE records set.
    		primaryRecord = new DataRecord(inPrimaryPort.getMetadata());
    		primaryRecord.init();
    		HashKey primaryHashKey = new HashKey(primaryKey, primaryRecord);
    		int numFields=defaultRecord.getNumFields();
            int keyFields[]=primaryKey.getKeyFields();
            
    		while (runIt && primaryRecord != null) {
    			try {
    				primaryRecord = inPrimaryPort.readRecord(primaryRecord);
    				if (primaryRecord != null) {
    					// let's find slave record
    					foreignRecord = (DataRecord) hashMap.get(primaryHashKey);
    					// do we have to fill default values ?
    					if (foreignRecord == null) {
                            for (int i=0; i < numFields; i++) {
                                foreignRecord.getField(keyFields[i]).copyFrom(defaultRecord.getField(i));
                            }
    					}
                        writeRecordBroadcast(primaryRecord);
    				}
    				SynchronizeUtils.cloverYield();
    			} catch (IOException ex) {
    				resultMsg = ex.getMessage();
    				resultCode = Node.RESULT_ERROR;
    				closeAllOutputPorts();
    				return;
    			} catch (Exception ex) {
    				resultMsg = ex.getMessage();
    				resultCode = Node.RESULT_FATAL_ERROR;
    				return;
    			}
    		}
    		broadcastEOF();
    		if (runIt) {
    			resultMsg = "OK";
    		} else {
    			resultMsg = "STOPPED";
    		}
    		resultCode = Node.RESULT_OK;
    	}
    
    
    	/**
    	 *  Description of the Method
    	 *
    	 * @return    Description of the Returned Value
    	 * @since     May 21, 2002
    	 */
    	public void toXML(Element xmlElement) {
    		super.toXML(xmlElement);
    		
    		if (primaryKeys != null) {
    			String jKeys = primaryKeys[0];
    			for (int i=1; i< primaryKeys.length; i++) {
    				jKeys += Defaults.Component.KEY_FIELDS_DELIMITER + primaryKeys[i]; 
    			}
    			xmlElement.setAttribute(XML_PRIMARYKEY_ATTRIBUTE, jKeys);
    		}
    		
    		if (foreignKeys != null) {
    			String overKeys = foreignKeys[0];
    			for (int i=1; i< foreignKeys.length; i++) {
    				overKeys += Defaults.Component.KEY_FIELDS_DELIMITER + foreignKeys[i]; 
    			}
    			xmlElement.setAttribute(XML_FOREIGNKEY_ATTRIBUTE, overKeys);
    		}
            
            if (defaultForeignKeys != null) {
                String overKeys = defaultForeignKeys[0];
                for (int i=1; i< defaultForeignKeys.length; i++) {
                    overKeys += Defaults.Component.KEY_FIELDS_DELIMITER + defaultForeignKeys[i]; 
                }
                xmlElement.setAttribute(XML_DEFAULTFOREIGNKEY_ATTRIBUTE, overKeys);
            }
    		
    		if (hashTableInitialCapacity > DEFAULT_HASH_TABLE_INITIAL_CAPACITY ) {
    			xmlElement.setAttribute(XML_HASHTABLESIZE_ATTRIBUTE, String.valueOf(hashTableInitialCapacity));
    		}
            
    	}
    
    
    	/**
    	 *  Description of the Method
    	 *
    	 * @param  nodeXML  Description of Parameter
    	 * @return          Description of the Returned Value
    	 * @since           May 21, 2002
    	 */
        @Override public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
    		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
            CheckForeignKeyNew checkKey;
    
    		try {
                checkKey = new CheckForeignKeyNew(
                        xattribs.getString(XML_ID_ATTRIBUTE),
                        xattribs.getString(XML_PRIMARYKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                        xattribs.getString(XML_FOREIGNKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                        xattribs.getString(XML_DEFAULTFOREIGNKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
    
    			if (xattribs.exists(XML_HASHTABLESIZE_ATTRIBUTE)) {
                    checkKey.setHashTableInitialCapacity(xattribs.getInteger(XML_HASHTABLESIZE_ATTRIBUTE));
    			}
    			return checkKey;
    		} catch (Exception ex) {
    	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
    		}
    	}
    	/**
    	 *  Description of the Method
    	 *
    	 * @return    Description of the Return Value
    	 */
    	public boolean checkConfig() {
    		return true;
    	}
    
    	public String getType(){
    		return COMPONENT_TYPE;
    	}
    
       
}

