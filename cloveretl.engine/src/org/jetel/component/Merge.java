/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component;

import java.io.*;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.Defaults;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.exception.ComponentNotReadyException;

/**
 *  <h3>Merge Component</h3> <!-- Merges data records from two input ports onto
 *  one output. It preserves sorted order (as specified by the merge key)
 *  The structure of records in all merged data flows must be the same - it implies
 *  that all input ports share the same metadata -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Merge</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Merges data records from two input ports onto
 *  one output. It preserves sorted order (as specified by the merge key)<br>
 *  The structure of records in all merged data flows must be the same - it implies
 *  that all input ports share the same metadata.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0..n]- input records (min 2 ports active)</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - one output port</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MERGE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>mergeKey</b></td><td>key which specifies the sort order to be preserved while merging</td>
 *  </tr>
 *  </table>  
 *
 *  <h4>Example:</h4> 
 *  <pre>&lt;Node id="MERGE" type="MERGE" mergeKey="name"/&gt;</pre>
 *
 *@author     dpavlis
 *@created    22. duben 2003
 *@since      April 22, 2003
 */
public class Merge extends Node {

	/**
	 *  Description of the Field
	 */
	public final static String COMPONENT_TYPE = "MERGE";

	private final static int WRITE_TO_PORT = 0;

	private DataRecord[] inputRecords;

	private String[] mergeKeys;

	private RecordKey comparisonKey;


	/**
	 *  Constructor for the Merge object
	 *
	 *@param  id         Description of the Parameter
	 *@param  mergeKeys  Description of the Parameter
	 */
	public Merge(String id, String[] mergeKeys) {
		super(id);
		this.mergeKeys = mergeKeys;
	}


	/**
	 *  Gets the Type attribute of the SimpleCopy object
	 *
	 *@return    The Type value
	 *@since     April 4, 2002
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}


	/**
	 *  Gets the first open/active port starting at specified index.
	 *  This is auxiliary function to simplify process of seeking
	 *  next port from which data can be read.
	 *
	 *@param  isEOF  Description of the Parameter
	 *@param  from   Description of the Parameter
	 *@return        The firstOpen value
	 */
	private int getFirstOpen(boolean[] isEOF, int from) {
		for (int i = from; i < isEOF.length; i++) {
			if (!isEOF[i]) {
				return i;
			}
		}
		return -1;
	}


	/**
	 *  From all input ports defined, selects the record whose
	 *  value (based on defined key) is lowest. It then returns
	 *  the index of that port
	 *
	 *@param  inputRecords  Description of the Parameter
	 *@param  isEOF         Description of the Parameter
	 *@return               The lowestRecIndex value
	 */
	private int getLowestRecIndex(DataRecord[] inputRecords, boolean[] isEOF) {
		int lowest;
		int compareTo;

		if ((lowest = getFirstOpen(isEOF, 0)) == -1) {
			return -1;
		}
		if ((compareTo = getFirstOpen(isEOF, lowest + 1)) == -1) {
			return lowest;
		}

		while (compareTo < isEOF.length && compareTo != -1) {
			if (comparisonKey.compare(inputRecords[lowest], inputRecords[compareTo]) == -1) {
				//lowest is lowest, no need to change
			} else {
				lowest = compareTo;
			}
			compareTo = getFirstOpen(isEOF, compareTo + 1);
		}

		return lowest;
	}



	/**
	 *  First time reads data from all input ports
	 *
	 *@param  inputRecords              Description of the Parameter
	 *@param  inPorts                   Description of the Parameter
	 *@param  isEOF                     Description of the Parameter
	 *@return                           Description of the Return Value
	 *@exception  IOException           Description of the Exception
	 *@exception  InterruptedException  Description of the Exception
	 */
	private int populateRecords(DataRecord[] inputRecords, InputPort[] inPorts, boolean[] isEOF)
			 throws IOException, InterruptedException {
		int numActive = 0;
		for (int i = 0; i < inPorts.length; i++) {
			if (inPorts[i].readRecord(inputRecords[i]) == null) {
				isEOF[i] = true;
			} else {
				numActive++;
			}
		}
		return numActive;
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 *@since    April 4, 2002
	 */
	public void run() {
		/*
		 *  we need to keep track of all input ports - if they contain data or
		 *  signalized that they are empty.
		 */
		InputPort inPorts[];
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		
		int numActive; 	// counter of still active ports - those without EOF status
		int readFromPort;
		int index;
		boolean[] isEOF = new boolean[getInPorts().size()];
		for (int i = 0; i < isEOF.length; i++) {
			isEOF[i] = false;
		}
		//get array of all input ports defined/connected - use collection Collection - getInPorts();
		inPorts = (InputPort[])getInPorts().toArray(new InputPort[0]);
		//create array holding incoming records
		inputRecords=new DataRecord[inPorts.length];

		// initialize array of data records (for each input port one)	
		for (int i = 0; i < inPorts.length; i++) {
			inputRecords[i] = new DataRecord(inPorts[i].getMetadata());
			inputRecords[i].init();
		}
		
		// initially load in records from all connected inputs
		try {
			numActive = populateRecords(inputRecords, inPorts, isEOF);
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
		

		// main merging loop - till there is some open port, try to
		// read and merge data from it
		while (runIt && numActive > 0) {
			index = getLowestRecIndex(inputRecords, isEOF);
			try {
				outPort.writeRecord(inputRecords[index]);
				inputRecords[index] = inPorts[index].readRecord(inputRecords[index]);
				if (inputRecords[index] == null) {
					numActive--;
					isEOF[index] = true;
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
		}
		setEOF(WRITE_TO_PORT);
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
	 *@exception  ComponentNotReadyException  Description of the Exception
	 *@since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		Class tClass;
		// test that we have at least one input port and one output
		if (inPorts.size() < 2) {
			throw new ComponentNotReadyException("At least two input ports have to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// initialize key
		comparisonKey = new RecordKey(mergeKeys, getInputPort(0).getMetadata());
		try {
			comparisonKey.init();
		} catch (Exception e) {
			throw new ComponentNotReadyException(e.getMessage());
		}
	}


	/**
	 *  Description of the Method
	 *
	 *@return    Description of the Returned Value
	 *@since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  nodeXML  Description of Parameter
	 *@return          Description of the Returned Value
	 *@since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);

		try{
			return new Merge(xattribs.getString("id"),
				xattribs.getString("mergeKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			return null;
		}
	}

}

