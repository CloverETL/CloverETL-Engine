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

import java.io.*;
import org.jetel.graph.*;
import java.util.HashSet;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.Defaults;
import org.jetel.data.SetVal;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.exception.ComponentNotReadyException;

/**
 *  <h3>CheckForeignKey Component</h3> <!--  Checks a foreign key against a table of 
 *	primary keys to see if the foreign key is valid. If the foreign key is invalid 
 *	then a default foreign key is substituted. The resulting foreign record is 
 *	broadcast to all connected outputs. The table containing the "primary" keys
 *	may contain duplicates but they will be ignored. -->
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
 *	Checks a foreign key against a table of 
 *	primary keys to see if the foreign key is valid. If the foreign key is invalid 
 *	then a default foreign key is substituted. The resulting foreign record is 
 *	broadcast to all connected outputs. The table containing the "primary" keys
 *	may contain duplicates but they will be ignored.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - primary records<br>
 *	  [1] - foreign records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [broadcast] - foreign records
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
 *		Name(s) of column(s) in input [0] that contain the primary key(s) 
 *		(field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>foreignKey</b></td><td>
 *		Name(s) of column(s) in input [1] that contain the foreign key(s) 
 *		(field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>defaultForeignKey</b></td><td>
 *		Key that should be substituted if the foreignKey doesn't exist in primaryKey.
 *		The order of the fields is as specified in foreignKeys
 *		(fields separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *    <tr><td><b>hashSize</b><br><i>optional</i></td><td>should be larger than the number of unique primary keys.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="CHECKFOREIGN" type="CHECK_FOREIGN_KEY" primaryKey="CustomerID" foreignKey="CustomerID" defaultForeignKey="-1"/&gt;</pre>
 *
 *  @since March 27, 2004
 */
public class CheckForeignKey extends Node {

	public final static String COMPONENT_TYPE = "CHECK_FOREIGN_KEY";

	private final static int DEFAULT_HASH_SET_INITIAL_CAPACITY = 512;
	private final static int PRIMARY_ON_PORT = 0;
	private final static int FOREIGN_ON_PORT = 1;

	private String[] primaryKeys;
	private String[] foreignKeys;
	private String[] defaultForeignKeys;
	private int hashSetInitialCapacity;
	
	public CheckForeignKey(String id, String[] primaryKeys, String[] foreignKeys, String[] defaultForeignKeys) {
		super(id);
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.defaultForeignKeys = defaultForeignKeys;
		this.hashSetInitialCapacity=DEFAULT_HASH_SET_INITIAL_CAPACITY;
	}

	public void setHashCapacity(int hashCapacity) {
		hashSetInitialCapacity = hashCapacity;
	}
	
	public void init() throws ComponentNotReadyException {
		if (inPorts.size() != 2) {
			throw new ComponentNotReadyException("Two input ports have to be defined.");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined.");
		}
		if (foreignKeys.length!=defaultForeignKeys.length) {
			throw new ComponentNotReadyException("foreignKey and defaultForeignKey must have the same number of items.");
		}
	}

	/**
	 *  Main processing method for the CheckForeignKey object
	 */
	public void run() {
		InputPort inPrimaryPort = getInputPort(PRIMARY_ON_PORT);
		InputPort inForeignPort = getInputPort(FOREIGN_ON_PORT);
		/* no output port because we broadcast to all connected outputs */
		DataRecord primaryRecord = new DataRecord(inPrimaryPort.getMetadata());
		DataRecord foreignRecord = new DataRecord(inForeignPort.getMetadata());
		RecordKey primaryKey = new RecordKey(primaryKeys, getInputPort(PRIMARY_ON_PORT).getMetadata());
		RecordKey foreignKey = new RecordKey(foreignKeys, getInputPort(FOREIGN_ON_PORT).getMetadata());
		HashSet hashSet = new HashSet(hashSetInitialCapacity);
		
		if (null==hashSet) {
			resultMsg = "Can't allocate HashSet of size: "+hashSetInitialCapacity;
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}
		
		primaryRecord.init();
		foreignRecord.init();
		primaryKey.init();
		foreignKey.init();

		// first read all records from PRIMARY port
		// and put the keystring into a hash
		try {
			for ( primaryRecord=inPrimaryPort.readRecord(primaryRecord); 
				runIt && primaryRecord!=null;
				primaryRecord=inPrimaryPort.readRecord(primaryRecord) ) {
			
				primaryKey.getKeyString(primaryRecord);
				// silently ignore duplicate primaryKeys
				hashSet.add(primaryKey.getKeyString(primaryRecord));	
			}
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
		

		// now read all records from FOREIGN port and try to look up their key 
		// in the PRIMARY key set. If it doesn't exist output the default key.
		try {
			for (foreignRecord=inForeignPort.readRecord(foreignRecord); 
				runIt && foreignRecord!=null;
				foreignRecord=inForeignPort.readRecord(foreignRecord)) {
			
				if (!hashSet.contains(foreignKey.getKeyString(foreignRecord))) {
					for (int i=0; i < foreignKeys.length; i++) {
						SetVal.setString(foreignRecord, foreignKeys[i], defaultForeignKeys[i]);
					}
				}
				writeRecordBroadcast(foreignRecord);
			}
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
		
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/**
	 * Description of the Method
	 *
	 * @return    Description of the Returned Value
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		// not yet implmented in the framework 
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		CheckForeignKey check;

		try {
			check = new CheckForeignKey(xattribs.getString("id"),
					xattribs.getString("primaryKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString("foreignKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString("defaultForeignKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			if (xattribs.exists("hashSize")) {
				check.setHashCapacity(xattribs.getInteger("hashSize"));
			}
			return check;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
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

