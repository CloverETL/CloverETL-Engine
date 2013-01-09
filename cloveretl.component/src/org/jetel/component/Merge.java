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

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.BasicComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

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
 * @author      dpavlis
 * @since       April 22, 2003
 * @revision    $Revision$
 * @created     22. duben 2003
 */
public class Merge extends Node {
	public final static String COMPONENT_TYPE = "MERGE";

	protected static final String XML_MERGEKEY_ATTRIBUTE = "mergeKey";

	private final static int WRITE_TO_PORT = 0;

	private DataRecord[] inputRecords;

	private String[] mergeKeys;

	private RecordKey comparisonKey;

	/**
	 *  Constructor for the Merge object
	 *
	 * @param  id         Description of the Parameter
	 * @param  mergeKeys  Description of the Parameter
	 */
	public Merge(String id, TransformationGraph graph) {
		super(id, graph);
	}


	/**
	 *  Gets the first open/active port starting at specified index.
	 *  This is auxiliary function to simplify process of seeking
	 *  next port from which data can be read.
	 *
	 * @param  isEOF  Description of the Parameter
	 * @param  from   Description of the Parameter
	 * @return        The firstOpen value
	 */
	private int getNextOpen(boolean[] isEOF, int from) {
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
	 * @param  inputRecords  Description of the Parameter
	 * @param  isEOF         Description of the Parameter
	 * @return               The lowestRecIndex value
	 */
	private int getLowestRecIndex(DataRecord[] inputRecords, boolean[] isEOF) {
		int lowest;
		int compareTo;

		if ((lowest = getNextOpen(isEOF, 0)) == -1) {
			return -1;
		}
		compareTo = getNextOpen(isEOF, lowest + 1);

		while (compareTo < isEOF.length && compareTo != -1) {
			if (comparisonKey.compare(inputRecords[lowest], inputRecords[compareTo]) == 1) {
				lowest = compareTo;// we have new lowest
			}
			compareTo = getNextOpen(isEOF, compareTo + 1);
		}

		return lowest;
	}



	/**
	 *  First time reads data from all input ports
	 *
	 * @param  inputRecords              Description of the Parameter
	 * @param  inPorts                   Description of the Parameter
	 * @param  isEOF                     Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
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

	@Override
	public Result execute() throws Exception {
		/*
		 *  we need to keep track of all input ports - if they contain data or
		 *  signalized that they are empty.
		 */
		InputPort inPorts[];
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);

		int numActive;// counter of still active ports - those without EOF status
		int index;

		//get array of all input ports defined/connected - use collection Collection - getInPorts();
		inPorts = (InputPort[]) getInPorts().toArray(new InputPort[0]);
		//create array holding incoming records
		inputRecords = new DataRecord[inPorts.length];
		boolean[] isEOF = new boolean[inPorts.length];
		for (int i = 0; i < isEOF.length; i++) {
			isEOF[i] = false;
		}
		// initialize array of data records (for each input port one)
		for (int i = 0; i < inPorts.length; i++) {
			inputRecords[i] = DataRecordFactory.newRecord(inPorts[i].getMetadata());
			inputRecords[i].init();
		}

		// initially load in records from all connected inputs
		numActive = populateRecords(inputRecords, inPorts, isEOF);
		// main merging loop - till there is some open port, try to
		// read and merge data from it
		while (runIt && numActive > 0) {
			index = getLowestRecIndex(inputRecords, isEOF);
			if (index != -1) {
				outPort.writeRecord(inputRecords[index]);
				inputRecords[index] = inPorts[index]
						.readRecord(inputRecords[index]);
				if (inputRecords[index] == null) {
					numActive--;
					isEOF[index] = true;
				}
			}
		}
		setEOF(WRITE_TO_PORT);
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		// initialize key
		comparisonKey = new RecordKey(mergeKeys, getInputPort(0).getMetadata());
		comparisonKey.setEqualNULLs(true);
		try {
			comparisonKey.init();
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, XML_MERGEKEY_ATTRIBUTE, e.getMessage());
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

		try {
			Merge merge = new Merge(xattribs.getString(XML_ID_ATTRIBUTE),graph);
			merge.setMergeKeys(xattribs.getString(XML_MERGEKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            return merge;
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
	    
	    if(!checkInputPorts(status, 1, Integer.MAX_VALUE)
	    		|| !checkOutputPorts(status, 1, 1)) {
	    	return status;
	    }

//this validation is commented out, because ClusterMerge component, which is descendant of this node, does not support multiple input edges 
//	    if (getInPorts().size() < 2) {
//	        status.add(new ConfigurationProblem("At least 2 input ports should be defined!", Severity.WARNING, this, Priority.NORMAL));
//	    }
	
	    checkMetadata(status, getInMetadata(), getOutMetadata(), false);
	
	    try {
	        init();
	    } catch (ComponentNotReadyException e) {
	        ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
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
	public String getType(){
		return COMPONENT_TYPE;
	}

	/**
	 * @return the mergeKeys
	 */
	public String[] getMergeKeys() {
		return mergeKeys;
	}

	/**
	 * @param mergeKeys the mergeKeys to set
	 */
	public void setMergeKeys(String[] mergeKeys) {
		this.mergeKeys = mergeKeys;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new BasicComponentTokenTracker(this);
	}
	
}

