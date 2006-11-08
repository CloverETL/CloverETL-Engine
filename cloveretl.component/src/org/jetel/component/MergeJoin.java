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
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Sorted Join Component</h3> <!-- Changes / reformats the data between
 *  pair of INPUT/OUTPUT ports This component is only a wrapper around
 *  transformation class implementing org.jetel.component.RecordTransform
 *  interface. The method transform is called for every record passing through
 *  this component -->
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>MergeJoin</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *  Joins sorted records on input ports. It expects driver stream at port [0] and 
 *  slave streams at other input ports.
 *  Each driver record is joined with corresponding slave records according
 *  to join keys specification.<br> 
 *	The method <i>transform</i> is every tuple composed of driver and corresponding
 *  slaves.<br>
 *  There are three join modes available: inner, left outer, full outer.<br>
 *  Inner mode processess only driver records for which all associated slaves are available.
 *  Left outer mode furthermore processes driver records with missing slaves.
 *  Full outer mode additionally calls transformation method for slaves without driver.<br>
 *  In case you use outer mode, be sure your transformation code is able to handle null
 *  input records.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - sorted driver record input<br>
 *	  [1+] - sorted slave record inputs<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - sole output port
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"MERGE_JOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>join key specification in format<br>
 *    <tt>[driver_key_list1]{*[slave_key_list1]|[slave_key_list1]|...}</tt><br>
 *    Each key list consists of comma-separated field names. In case any slave key list is missing,
 *    the component will use the sole driver key list instead of it.
 *    Order of slave key lists corresponds to order of slave input ports.
 *    </td></tr>
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming joined data specified in <tt>transformClass<tt> parameter.</td></tr>
 *   <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td></tr>
 *    <tr><td><b>joinType</b><br><i>optional</i></td><td>inner/leftOuter/fullOuter Specifies type of join operation. Default is inner.</td></tr>
 *    <tr><td><b>slaveDuplicates</b><br><i>optional</i></td><td>true/false - allow records on slave port with duplicate keys. Default is false - multiple
 *    duplicate records are discarded - only the last one is used for join.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="JOIN" type="MERGE_JOIN" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="JOIN" type="HASH_JOIN" joinKey="EmployeeID*EmployeeID" joinType="inner"&gt;
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
 * @author      dpavlis, Jan Hadrava
 * @since       April 4, 2002
 * @revision    $Revision$
 * @created     4. June 2003
 */
/**
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class MergeJoin extends Node {
	public enum Join {
		INNER,
		LEFT_OUTER,
		FULL_OUTER,
	}

	private static final String XML_FULLOUTERJOIN_ATTRIBUTE = "fullOuterJoin";
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";
	private static final String XML_SLAVEOVERRIDEKEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_JOINTYPE_ATTRIBUTE = "joinType";
	private static final String XML_JOINKEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE ="slaveDuplicates";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "MERGE_JOIN";

	private final static int WRITE_TO_PORT = 0;
	private final static int DRIVER_ON_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;

	private String transformClassName;
	private String transformSource = null;

	private RecordTransform transformation = null;

	private DataRecord[] inRecords;
	private DataRecord[] outRecords;

	private Properties transformationParameters;
	
//	private static Log logger = LogFactory.getLog(MergeJoin.class);
	
	static Log logger = LogFactory.getLog(HashJoin.class);

	private String[][] joiners;
	private Join join;

	private boolean slaveDuplicates;

	private int inputCnt;
	private int slaveCnt;
	
	private RecordKey driverKey;
	private RecordKey[] slaveKeys;

	InputReader[] reader;
	InputReader minReader;
	boolean[] minIndicator;
	int minCnt;

	OutputPort outPort;

	/**
	 *  Constructor for the SortedJoin object
	 *
	 * @param id		id of component
	 * @param joiners	parsed join string (first element contains driver key list, following elements contain slave key lists)
	 * @param transform
	 * @param transformClass  class (name) to be used for transforming data
	 * @param join join type
	 * @param slaveDuplicates enables/disables duplicate slaves
	 */
	public MergeJoin(String id, String[][] joiners, String transform,
			String transformClass, Join join, boolean slaveDuplicates) {
		super(id);
		this.joiners = joiners;
		this.transformSource = transform;
		this.transformClassName = transformClass;
		this.join = join;
		this.slaveDuplicates = slaveDuplicates;
	}

	/**
	 * Replace minimal record runs with the following ones. Change min indicator array
	 * to reflect new set of runs.
	 * @return Number of minimal runs, zero when no more data are available.
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private int loadNext() throws InterruptedException, IOException {
		minCnt = 0;
		int minIdx = 0;
		for (int i = 0; i < inputCnt; i++) {
			if (minIndicator[i]) {
				reader[i].loadNextRun();
			}
			switch (reader[minIdx].compare(reader[i])) {
			case -1: // current is greater than minimal
				minIndicator[i] = false;
				break;
			case 0: // current is equal to minimal
				minCnt++;
				minIndicator[i] = true;
				break;
			case 1: // current is lesser than minimal
				minCnt = 1;
				minIdx = i;
				minIndicator[i] = true;
				break; // all previous indicators will be reset later
			}
		} // for
		for (int i = minIdx - 1; i >= 0; i--) {
			minIndicator[i] = false;
		}
		minReader = reader[minIdx];
		if (reader[minIdx].getSample() == null) {	// no more data
			minCnt = 0;
		}
		return minCnt;
	}

	/**
	 * Tranform all tuples created from minimal input runs.
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean flushMin() throws IOException, InterruptedException {
		// create initial combination
		for (int i = 0; i < inputCnt; i++) {
				inRecords[i] = minIndicator[i] ? reader[i].next() : null;
		}
		while (true) {
			outRecords[0].reset();
			if (!transformation.transform(inRecords, outRecords)) {
				return false;
			}
			outPort.writeRecord(outRecords[0]);
			// generate next combination
			int chngCnt = 0;
			for (int i = inputCnt - 1; i >= 0; i--) {
				if (!minIndicator[i]) {
					continue;
				}
				chngCnt++;
				inRecords[i] = reader[i].next();
				if (inRecords[i] != null) {	// have new combination
					break;
				}
				if (chngCnt == minCnt) { // no more combinations available
					return true;
				}
				// need rewind
				reader[i].rewindRun();
				inRecords[i] = reader[i].next();	// this is supposed to return non-null value
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	public void run() {
		try {
			for (loadNext(); minCnt > 0; loadNext()) {
				if (join == Join.INNER && minCnt != inputCnt) { // not all records for current key available
					continue;
				}
				if (join == Join.LEFT_OUTER && !minIndicator[0]) {	// driver record for current key not available
					continue;
				}
				if (!flushMin()) {
					resultCode = Node.RESULT_ERROR;
					resultMsg = transformation.getMessage();
					transformation.finished();
					broadcastEOF();
					return;
				}
			}
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		// signal end of records stream to transformation function
		transformation.finished();
		broadcastEOF();		
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port have to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		inputCnt = inPorts.size();
		slaveCnt = inputCnt - 1;
		if (joiners.length < 1) {
			throw new ComponentNotReadyException("not enough join keys specified");
		} else if (joiners.length < inputCnt) {
			logger.warn("Join keys aren't specified for all slave inputs - deducing missing keys");
			String[][] replJoiners = new String[inputCnt][];
			for (int i = 0; i < joiners.length; i++) {
				replJoiners[i] = joiners[i];
			}
			// use driver key list for all missing slave key specifications
			for (int i = joiners.length; i < inputCnt; i++) {
				replJoiners[i] = joiners[0];
			}
			joiners = replJoiners;
		}
		driverKey = new RecordKey(joiners[0], getInputPort(DRIVER_ON_PORT).getMetadata());
		driverKey.init();
		slaveKeys = new RecordKey[slaveCnt];
		for (int idx = 0; idx < slaveCnt; idx++) {
			slaveKeys[idx] = new RecordKey(joiners[1 + idx], getInputPort(FIRST_SLAVE_PORT + idx).getMetadata());
			slaveKeys[idx].init();
		}
		reader = new InputReader[inputCnt];
		reader[0] = new DriverReader(getInputPort(DRIVER_ON_PORT), driverKey);
		if (slaveDuplicates) {
			for (int i = 0; i < slaveCnt; i++) {
				reader[i + 1] = new SlaveReaderDup(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i]);
			}
		} else {
			for (int i = 0; i < slaveCnt; i++) {
				reader[i + 1] = new SlaveReader(getInputPort(FIRST_SLAVE_PORT + i), slaveKeys[i]);
			}			
		}
		minReader = reader[0];
		minIndicator = new boolean[inputCnt];
		for (int i = 0; i < inputCnt; i++) {
			minIndicator[i] = true;
		}
		inRecords = new DataRecord[inputCnt];
		outRecords = new DataRecord[]{new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata())};
		outRecords[0].init();
		outPort = getOutputPort(WRITE_TO_PORT);
		// init transformation
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] {
				getOutputPort(WRITE_TO_PORT).getMetadata()};
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[inputCnt];
		for (int idx = 0; idx < inputCnt; idx++) {
			inMetadata[idx] = getInputPort(idx).getMetadata();
		}
        try {
            transformation = RecordTransformFactory.createTransform(
            		transformSource, transformClassName, this, inMetadata, outMetadata, transformationParameters);
        } catch(Exception e) {
            throw new ComponentNotReadyException(this, e);
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
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, transformClassName);
		} 
		
		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}
		
		String joinStr = "";
		for (int i = 0; true; i++) {
			for (int j = 0; true; j++) {
				joinStr += joiners[i][j];
				if (j == joiners[i].length - 1) {
					break;	// leave inner loop
				}
				joinStr += ",";
			}
			if (i == joiners.length - 1) {
				break;
			}
			joinStr += i == 0 ? "*" : "|";
		}

		xmlElement.setAttribute(XML_JOINKEY_ATTRIBUTE, joinStr);
		
		xmlElement.setAttribute(XML_JOINTYPE_ATTRIBUTE,
				join == Join.FULL_OUTER ? "fullOuter" : join == Join.LEFT_OUTER ? "leftOuter" : "inner");

		xmlElement.setAttribute(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, String.valueOf(slaveDuplicates));

		if (transformationParameters != null) {
			Enumeration propertyAtts = transformationParameters.propertyNames();
			while (propertyAtts.hasMoreElements()) {
				String attName = (String)propertyAtts.nextElement();
				xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
			}
		}		
	}

	/**
	 * Parses join string.
	 * @param joinBy Join string
	 * @return Array of arrays of strings. Each subarray represents one driver/slave key list
	 * @throws XMLConfigurationException
	 */
	private static String[][] parseJoiners(String joinBy) throws XMLConfigurationException {
		String[] spl = joinBy.split("\\*", 2);
		String[] slaveKeys = new String[0];
		if (spl.length > 1) {
			slaveKeys = spl[1].split("\\|");
		}
		String[] keys = new String[1 + slaveKeys.length];
		keys[0] =  spl[0];
		for (int i = 0; i < slaveKeys.length; i++) {
			keys[1 + i] = slaveKeys[i];
		}
		String[][] res = new String[keys.length][];

		for (int i = 0; i < keys.length; i++) {
			res[i] = keys[i].split(",");
		}
		return res;
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
		MergeJoin join;

		try {
			String joinStr = xattribs.getString(XML_JOINTYPE_ATTRIBUTE, "inner");
			Join joinType;
			if (joinStr == null || joinStr.equalsIgnoreCase("inner")) {
				joinType = Join.INNER;
			} else if (joinStr.equalsIgnoreCase("leftOuter")) {
				joinType = Join.LEFT_OUTER;
			} else if (joinStr.equalsIgnoreCase("fullOuter")) {
				joinType = Join.FULL_OUTER;
			} else {
				throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" 
						+ "Invalid joinType specification: " + joinStr);				
			}

			String[][] joiners = parseJoiners(xattribs.getString(XML_JOINKEY_ATTRIBUTE, ""));

            join = new MergeJoin(
                    xattribs.getString(XML_ID_ATTRIBUTE),
                    joiners,
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
                    joinType,
                    xattribs.getBoolean(XML_ALLOW_SLAVE_DUPLICATES_ATTRIBUTE, true));
			join.setTransformationParameters(xattribs.attributes2Properties(
	                new String[]{XML_ID_ATTRIBUTE,XML_JOINKEY_ATTRIBUTE,
	                		XML_TRANSFORM_ATTRIBUTE,XML_TRANSFORMCLASS_ATTRIBUTE,
	                		XML_LEFTOUTERJOIN_ATTRIBUTE,XML_SLAVEOVERRIDEKEY_ATTRIBUTE,
	                		XML_FULLOUTERJOIN_ATTRIBUTE}));			
			return join;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	/**
	 * Interface specifying operations for reading ordered record input.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private interface InputReader {
		/**
		 * Loads next run (set of records with identical keys)  
		 * @return
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public boolean loadNextRun() throws InterruptedException, IOException;

		/**
		 * Retrieves one record from current run. Modifies internal data so that next call
		 * of this operation will return following record. 
		 * @return null on end of run, retrieved record otherwise
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public DataRecord next() throws IOException, InterruptedException;

		/**
		 * Resets current run so that it can be read again.  
		 */
		public void rewindRun();
		
		/**
		 * Retrieves one record from current run. Doesn't affect results of sebsequent next() operations.
		 * @return
		 */
		public DataRecord getSample();

		/**
		 * Returns key used to compare data records. 
		 * @return
		 */
		public RecordKey getKey();
		
		/**
		 * Compares reader with another one. The comparison is based on key values of record in the current run
		 * @param other
		 * @return
		 */
		public int compare(InputReader other);
	}

	/**
	 * Reader for driver input. Doesn't use record buffer but also doesn't support rewind operation.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class DriverReader implements InputReader {
		private static final int CURRENT = 0;
		private static final int NEXT = 1;

		private InputPort inPort;
		private RecordKey key;
		private DataRecord[] rec = new DataRecord[2];
		private int recCounter;
		private boolean blocked;

		public DriverReader(InputPort inPort, RecordKey key) {
			this.inPort = inPort;
			this.key = key;
			this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
			this.rec[NEXT] = new DataRecord(inPort.getMetadata());
			this.rec[CURRENT].init();
			this.rec[NEXT].init();
			recCounter = 0;
			blocked = false;
		}

		public boolean loadNextRun() throws InterruptedException, IOException {
			if (inPort == null) {
				return false;
			}
			if (recCounter == 0) {	// first call of this function
				// load first record of the run
				if (inPort.readRecord(rec[NEXT]) == null) {
					inPort = null;
					rec[NEXT] = rec[CURRENT] = null;
					return false;
				}
				recCounter = 1;
				return true;
			}

			if (blocked) {
				blocked = false;
				return true;
			}
			do {
				swap();
				if (inPort.readRecord(rec[NEXT]) == null) {
					inPort = null;
					rec[NEXT] = rec[CURRENT] = null;
					return false;
				}
				recCounter++;
			} while (key.compare(rec[CURRENT], rec[NEXT]) == 0);
			return true;
		}

		public void rewindRun() {
			throw new UnsupportedOperationException();
		}

		public DataRecord getSample() {
			return blocked ? rec[CURRENT] : rec[NEXT];
		}

		public DataRecord next() throws IOException, InterruptedException {
			if (blocked || inPort == null) {
				return null;
			}
			swap();
			if (inPort.readRecord(rec[NEXT]) == null) {
				inPort = null;
				rec[NEXT] = null;
				blocked = false;
			} else {
				recCounter++;
				blocked = key.compare(rec[CURRENT], rec[NEXT]) != 0;
			}
			return rec[CURRENT];
		}
		
		private void swap() {
			DataRecord tmp = rec[CURRENT];
			rec[CURRENT] = rec[NEXT];
			rec[NEXT] = tmp;
		}

		public RecordKey getKey() {
			return key;
		}
		
		public int compare(InputReader other) {
			DataRecord rec1 = getSample();
			DataRecord rec2 = other.getSample();
			if (rec1 == null) {
				return rec2 == null ? 0 : 1;	// null is greater than any other reader
			} else if (rec2 == null) {
				return -1;
			}
			return key.compare(other.getKey(), rec1, rec2);
		}

	}
	
	/**
	 * Slave reader with duplicates support. Uses file buffer to store duplicate records. Support rewind operation.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class SlaveReaderDup implements InputReader {
		private static final int CURRENT = 0;
		private static final int NEXT = 1;

		private InputPort inPort;
		private RecordKey key;
		private DataRecord[] rec = new DataRecord[2];
		private FileRecordBuffer recBuf;
		private ByteBuffer rawRec = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		private boolean firstRun;
		private boolean getFirst;
		DataRecord deserializedRec;

		public SlaveReaderDup(InputPort inPort, RecordKey key) {
			this.inPort = inPort;
			this.key = key;
			this.deserializedRec = new DataRecord(inPort.getMetadata());
			this.deserializedRec.init();
			this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
			this.rec[NEXT] = new DataRecord(inPort.getMetadata());
			this.rec[CURRENT].init();
			this.rec[NEXT].init();
			this.recBuf = new FileRecordBuffer(null);
			this.firstRun = true;
		}
		
		private void swap() {
			DataRecord tmp = rec[CURRENT];
			rec[CURRENT] = rec[NEXT];
			rec[NEXT] = tmp;
		}

		public boolean loadNextRun() throws InterruptedException, IOException {
			getFirst = true;
			if (inPort == null) {
				rec[CURRENT] = rec[NEXT] = null;
				return false;
			} 
			if (firstRun) {	// first call of this function
				firstRun = false;
				// load first record of the run
				if (inPort.readRecord(rec[NEXT]) == null) {
					rec[CURRENT] = rec[NEXT] = null;
					inPort = null;
					return false;
				}
			}
			recBuf.clear();
			swap();
			while (true) {
				rec[NEXT].reset();
				if (inPort.readRecord(rec[NEXT]) == null) {
					rec[NEXT] = null;
					inPort = null;
					return true;
				}
				if (key.compare(rec[CURRENT], rec[NEXT]) != 0) {	// beginning of new run
					return true;
				}
				// move record to buffer
				rawRec.clear();
				rec[NEXT].serialize(rawRec);
				rawRec.flip();
				recBuf.push(rawRec);				
			}
		}

		public void rewindRun() {
			getFirst = true;
			recBuf.rewind();
		}

		public DataRecord getSample() {
			if (firstRun) {
				return null;
			}
			return rec[CURRENT];
		}

		public DataRecord next() throws IOException {
			if (firstRun) {
				return null;
			}
			if (getFirst) {
				getFirst = false;
				return rec[CURRENT];
			}
			rawRec.clear();
			if (recBuf.shift(rawRec) == null) {
				return null;
			}
			rawRec.flip();
			deserializedRec.deserialize(rawRec);
			return deserializedRec;
		}

		public RecordKey getKey() {
			return key;
		}
		
		public int compare(InputReader other) {
			DataRecord rec1 = getSample();
			DataRecord rec2 = other.getSample();
			if (rec1 == null) {
				return rec2 == null ? 0 : -1;
			} else if (rec2 == null) {
				return 1;
			}
			return key.compare(other.getKey(), rec1, rec2);
		}
	}

	/**
	 * Slave reader without duplicates support. Pretends that all runs contain only one record.
	 * Doesn't use buffer, supports rewind operation.
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 *
	 */
	private static class SlaveReader implements InputReader {
		private static final int CURRENT = 0;
		private static final int NEXT = 1;

		private InputPort inPort;
		private RecordKey key;
		private DataRecord[] rec = new DataRecord[2];
		private boolean firstRun;
		private boolean needsRewind;

		public SlaveReader(InputPort inPort, RecordKey key) {
			this.inPort = inPort;
			this.key = key;
			this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
			this.rec[NEXT] = new DataRecord(inPort.getMetadata());
			this.rec[CURRENT].init();
			this.rec[NEXT].init();
			this.firstRun = true;
			this.needsRewind = true;
		}
		
		private void swap() {
			DataRecord tmp = rec[CURRENT];
			rec[CURRENT] = rec[NEXT];
			rec[NEXT] = tmp;
		}

		public boolean loadNextRun() throws InterruptedException, IOException {
			if (inPort == null) {
				rec[CURRENT] = rec[NEXT] = null;
				return false;
			} 
			if (firstRun) {	// first call of this function
				firstRun = false;
				// load first record of the run
				if (inPort.readRecord(rec[NEXT]) == null) {
					rec[CURRENT] = rec[NEXT] = null;
					inPort = null;
					return false;
				}
			}
			swap();
			while (true) {
			// current record is now the first one from the run to be loaded
			// set current record to the last one from the run to be loaded and next record to the first one
			// from the following run
			needsRewind = false;
				rec[NEXT].reset();
				if (inPort.readRecord(rec[NEXT]) == null) {
					rec[NEXT] = null;
					inPort = null;
					return true;
				}
				if (key.compare(rec[CURRENT], rec[NEXT]) != 0) {	// beginning of new run
					return true;
				}
				swap();
			}
		}

		public void rewindRun() {
		}

		public DataRecord getSample() {
			if (firstRun) {
				return null;
			}
			return rec[CURRENT];
		}

		public DataRecord next() throws IOException {
			if (firstRun || needsRewind) {
				return null;
			}
			needsRewind = true;
			return rec[CURRENT];
		}

		public RecordKey getKey() {
			return key;
		}
		
		public int compare(InputReader other) {
			DataRecord rec1 = getSample();
			DataRecord rec2 = other.getSample();
			if (rec1 == null) {
				return rec2 == null ? 0 : -1;
			} else if (rec2 == null) {
				return 1;
			}
			return key.compare(other.getKey(), rec1, rec2);
		}
	}

}
