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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

 /**
 * <h3>CheckForeignKey Component</h3> <!--  Checks a defined foreign key against a table of 
 *  primary keys to verify whether the foreign key is valid. If the foreign key is not
 *  found among primary keys, default foreign key is substituted. The resulting foreign record is 
 *  sent to output port 0. The table containing the "primary" keys
 *  may contain duplicates but they will be ignored. -->
 *
 * <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>CheckForeignKey</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *  Checks a defined foreign key against a table of 
 *  primary keys to verify whether the foreign key is valid. If the foreign key is not
 *  found among primary keys, default foreign key is substituted. The resulting foreign record is 
 *  sent to output port 0. The table containing the "primary" keys
 *  may contain duplicates but they will be ignored.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
  *    	[0] - foreign records<br>
 *      [1] - primary records<br>
*    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - foreign records with eventually new key<br>
 *        [1] - (optional) foreign records with invalid key 
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"CHECK_FOREIGN_KEY"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>primaryKey</b></td><td>
 *      Name(s) of column(s) in input [1] that contain the primary key(s) 
 *      (field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>foreignKey</b></td><td>
 *      Name(s) of column(s) in input [0] that contain the foreign key(s) 
 *      (field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>defaultForeignKey</b></td><td>
 *      Key that should be substituted if the foreignKey doesn't exist in primaryKey.
 *      The order of the fields is as specified in foreignKeys
 *      (fields separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>hashSize</b><br><i>optional</i></td><td>should be larger than the number of unique primary keys.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="CHECKFOREIGN" type="CHECK_FOREIGN_KEY" primaryKey="CustomerID" 
 *    foreignKey="CustomerID" defaultForeignKey="-1"/&gt;</pre>
 *
 *	  
 * @author      david pavlis
 * @since       Sep, 2006
 * @revision    $Revision: $
 * @created     Sep, 2006
 */
    public class CheckForeignKey extends Node {
    
    	private static final String XML_HASHTABLESIZE_ATTRIBUTE = "hashTableSize";
        
        public static final String XML_FOREIGNKEY_ATTRIBUTE = "foreignKey";
        private static final String XML_PRIMARYKEY_ATTRIBUTE = "primaryKey";
        private static final String XML_DEFAULTFOREIGNKEY_ATTRIBUTE = "defaultForeignKey";
    
    	/**  Description of the Field */
        public final static String COMPONENT_TYPE = "CHECK_FOREIGN_KEY";
    
    	private final static int DEFAULT_HASH_TABLE_INITIAL_CAPACITY = Defaults.Lookup.LOOKUP_INITIAL_CAPACITY;
    
    	private final static int WRITE_TO_PORT = 0; 
    	private final static int REJECTED_PORT = 1;
        private final static int PRIMARY_ON_PORT = 1;
        private final static int FOREIGN_ON_PORT = 0;
    
    
    	private String[] primaryKeys;
    	private String[] foreignKeys;
        private String[] defaultForeignKeys;
        private DataRecord defaultRecord;
        
    	private RecordKey primaryKey;
    	private RecordKey foreignKey;
    
    	private Map<HashKey, DataRecord> hashMap;
    	private int hashTableInitialCapacity;
    	
    
    	static Log logger = LogFactory.getLog(CheckForeignKey.class);
    
    	/**
    	 *Constructor for the HashJoin object
    	 *
    	 * @param  id              Description of the Parameter
    	 * @param  joinKeys        Description of the Parameter
    	 * @param  transformClass  Description of the Parameter
    	 * @param  leftOuterJoin   Description of the Parameter
    	 */
    	public CheckForeignKey(String id, String[] primaryKeys, String[] foreignKeys, 
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
            if(isInitialized()) return;
    		super.init();

    		(primaryKey = new RecordKey(primaryKeys, getInputPort(PRIMARY_ON_PORT)
                    .getMetadata())).init();
            (foreignKey = new RecordKey(foreignKeys,
                    getInputPort(FOREIGN_ON_PORT).getMetadata())).init();
    
            // allocate HashMap
            try {
                hashMap = new HashMap<HashKey, DataRecord>(hashTableInitialCapacity);
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
            defaultRecord = new DataRecord(foreignKey.generateKeyRecordMetadata());
            defaultRecord.init();
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
    
        @Override
        public Result execute() throws Exception {
        	OutputPort rejectedPort = getOutputPort(REJECTED_PORT);
    		InputPort inPrimaryPort = getInputPort(PRIMARY_ON_PORT);
    		InputPort inForeignPort = getInputPort(FOREIGN_ON_PORT);
    		DataRecord primaryRecord;
    		DataRecord foreignRecord;
    		DataRecord storeRecord;
    
    		primaryRecord=new DataRecord(inPrimaryPort.getMetadata());
    		primaryRecord.init();
    		while (primaryRecord!=null && runIt) {
   				if ((primaryRecord=inPrimaryPort.readRecord(primaryRecord)) != null) {
   				    storeRecord=primaryRecord.duplicate();
   					hashMap.put(new HashKey(primaryKey, storeRecord), storeRecord);
   				} 
   				SynchronizeUtils.cloverYield();
    		}

    		foreignRecord = new DataRecord(inForeignPort.getMetadata());
    		foreignRecord.init();
    		HashKey foreignHashKey = new HashKey(foreignKey, foreignRecord);
    		int numFields=defaultRecord.getNumFields();
            int keyFields[]=foreignKey.getKeyFields();
    		while (runIt && foreignRecord != null) {
   				foreignRecord = inForeignPort.readRecord(foreignRecord);
   				if (foreignRecord != null) {
   					// let's find slave record
   					primaryRecord = (DataRecord) hashMap.get(foreignHashKey);
   					// do we have to fill default values ?
   					if (primaryRecord == null) {
						if (rejectedPort != null) {
							writeRecord(REJECTED_PORT, foreignRecord);
						}   						
						for (int i=0; i < numFields; i++) {
                           foreignRecord.getField(keyFields[i]).setValue(defaultRecord.getField(i));
                       }
   					}
                    writeRecord(WRITE_TO_PORT, foreignRecord);
   				}
   				SynchronizeUtils.cloverYield();
    		}
    		broadcastEOF();
            return runIt ? Result.FINISHED_OK : Result.ABORTED;
       }
    
       /*
        * (non-Javadoc)
        * @see org.jetel.graph.Node#reset()
        */
    	@Override
		public synchronized void reset() throws ComponentNotReadyException {
			super.reset();
			hashMap.clear();
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
        public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
    		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
            CheckForeignKey checkKey;
    
    		try {
                checkKey = new CheckForeignKey(
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
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
            super.checkConfig(status);

    		if(!checkInputPorts(status, 2, 2)
    				|| !checkOutputPorts(status, 1, 2)) {
    			return status;
    		}

    		DataRecordMetadata primaryMetadata = getInputPort(PRIMARY_ON_PORT).getMetadata();
    		DataRecordMetadata foreignMetadata = getInputPort(FOREIGN_ON_PORT).getMetadata();

        	checkMetadata(status, foreignMetadata, getOutMetadata());

        	primaryKey = new RecordKey(primaryKeys, primaryMetadata);
        	foreignKey = new RecordKey(foreignKeys,foreignMetadata);
        	RecordKey.checkKeys(primaryKey, XML_PRIMARYKEY_ATTRIBUTE, foreignKey, 
        			XML_FOREIGNKEY_ATTRIBUTE, status, this);
            
            return status;
        }
    
    	public String getType(){
    		return COMPONENT_TYPE;
    	}
    
       
}

