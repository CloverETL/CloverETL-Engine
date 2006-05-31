/**
 * 
 */
package org.jetel.component;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.SortDataRecordInternal;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;

/**
 * @author avackova
 *
 */
public class ReferenceMatchingSort extends Node {

	private static final String XML_SORTORDER_ATTRIBUTE = "sortOrder";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "REF_MATCH_SORT";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private SortDataRecordInternal sorter;
	private boolean sortOrderAscending;
	private Key[] sortKeys;
	private ByteBuffer recordBuffer;
	private int[][] fieldMap;
	private int outKey;
	private InputPort inPort;
	private DataRecordMetadata inMetadata;
	private OutputPort outPort;
	private DataRecordMetadata outMetadata;
	
	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true; 

	/**
	 * @param id
	 */
	public ReferenceMatchingSort(String id, Key[] sortKeys, boolean sortOrder) {
		super(id);
		this.sortOrderAscending = sortOrder;
		this.sortKeys = sortKeys;
	}

	public ReferenceMatchingSort(String id, Key[] sortKeys) {
		this(id,sortKeys,DEFAULT_ASCENDING_SORT_ORDER);
	}

	/**
	 * @param in 		metadata on input port
	 * @param out		metadata on output port
	 * @param fieldMap	int[in.getNumFields][2] - array in which are stored field's numbers from in and field's numbers on out
	 * @return number of field in out which does not exist in in
	 * @throws JetelException
	 */
	private int mapFields(DataRecordMetadata in,DataRecordMetadata out,int[][] fieldMap) throws ComponentNotReadyException{
		if (!(out.getNumFields()==in.getNumFields()+1))
			throw new ComponentNotReadyException("Metadata on output does not correspond with metadata on input!");
		int r=0;
		int i;
		for (i=0;i<out.getNumFields();i++){
			int j;
			for (j=0;j<in.getNumFields();j++){
				if (out.getField(i).getName().equals(in.getField(j).getName())){
					fieldMap[i][0]=j;
					fieldMap[i][1]=i;
					break;
				}
			}
			if (j==in.getNumFields())
				r=j;
		}
		return r;
	}
	
	private void fillOutRecord(DataRecord inRecord,DataRecord outRecord,int[][] map,int num,String value){
		for (int i=0;i<map.length;i++){
			outRecord.getField(map[i][1]).setValue(inRecord.getField(map[i][0]).getValue());
		}
		outRecord.getField(num).setValue(value);
	}
	
	public void run() {
		DataRecord inRecord = new DataRecord(inMetadata);
		inRecord.init();
		DataRecord outRecord = new DataRecord(outMetadata);
		outRecord.init();
		int j=0;
		for (int i=0;i<sortKeys.length;i++){
			j+=sortKeys[i].getEnd()-sortKeys[i].getStart();
		}
		StringBuffer resultString=new StringBuffer(j);
		int licz=0;
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);// readRecord(READ_FROM_PORT,inRecord);
				if (inRecord!=null) {
					resultString.setLength(0);
					for (int i=0;i<sortKeys.length;i++){
						String pom=inRecord.getField(sortKeys[i].getName()).getValue().toString();
						resultString.append(pom.substring(sortKeys[i].getStart(),sortKeys[i].getEnd()));
					}
					fillOutRecord(inRecord,outRecord,fieldMap,outKey,resultString.toString());
					if(!sorter.put(outRecord)){
					    System.err.println("Sorter "+getID()+" has no more capacity to sort additional records." +
					    		"The output will be incomplete !");
					    break; // no more capacity
					}
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				ex.printStackTrace();
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
				return;
			}
		}
		// --- sort the records now
		try {
				sorter.sort();
		} catch (Exception ex) {
			resultMsg = "Error when sorting: " + ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		// --- read sorted records
		while (sorter.get(recordBuffer) && runIt) {
		    try {
		        writeRecordBroadcastDirect(recordBuffer);
		        recordBuffer.clear();
		    } catch (IOException ex) {
		        resultMsg = ex.getMessage();
		        resultCode = Node.RESULT_ERROR;
		        closeAllOutputPorts();
		        return;
		    } catch (Exception ex) {
		        resultMsg = ex.getMessage();
		        resultCode = Node.RESULT_FATAL_ERROR;
		        //closeAllOutputPorts();
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
	 *  Sets the sortOrderAscending attribute of the Sort object
	 *
	 * @param  ascending  The new sortOrderAscending value
	 */
	public void setSortOrderAscending(boolean ascending) {
		sortOrderAscending = ascending;
	}

	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		if (recordBuffer == null) {
			throw new ComponentNotReadyException("Can NOT allocate internal record buffer ! Required size:" +
					Defaults.Record.MAX_RECORD_SIZE);
		}
		// create sorter
		inPort = getInputPort(READ_FROM_PORT);
		inMetadata=inPort.getMetadata();
		outPort = getOutputPort(WRITE_TO_PORT);
		outMetadata=outPort.getMetadata();
		fieldMap=new int[inMetadata.getNumFields()][2];
		outKey=mapFields(inMetadata,outMetadata,fieldMap);
		String[] sortKeys={outMetadata.getField(outKey).getName()};
		sorter = new SortDataRecordInternal(outMetadata, sortKeys, sortOrderAscending);
	}

	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		ReferenceMatchingSort result;
		try {
			String[] keys=xattribs.getString(XML_SORTKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			Key[] sortKeys=new Key[keys.length];
			for (int i=0;i<keys.length;i++){
				String[] pom=keys[i].split(" ");
				switch (pom.length) {
					case 2:sortKeys[i]=new Key(pom[0],Integer.parseInt(pom[1]));
						break;
					case 3:sortKeys[i]=new Key(pom[0],Integer.parseInt(pom[1]),Integer.parseInt(pom[2]));
						break;
					default:	System.err.println(COMPONENT_TYPE + ":wrong format of XML_SORTKEY_ATTRIBUTE" );
						return null;
				}
			}
			result = new ReferenceMatchingSort(xattribs.getString(Node.XML_ID_ATTRIBUTE),sortKeys);
			if (xattribs.exists(XML_SORTORDER_ATTRIBUTE)) {
				result.setSortOrderAscending(xattribs.getString(XML_SORTORDER_ATTRIBUTE).matches("^[Aa].*"));
			}
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
		return result;
	}

	public boolean checkConfig() {
		return true;
	}

	public String getType(){
		return COMPONENT_TYPE;
	}

	static private class Key{
		
		String name;
		int start;
		int end;
		
		Key(String name,int start,int end){
			this.name=name;
			this.start=start;
			this.end=end;
		}
		
		Key(String name,int length){
			this.name=name;
			this.start=0;
			this.end=length;
		}

		public int getEnd() {
			return end;
		}

		public String getName() {
			return name;
		}

		public int getStart() {
			return start;
		}
	}
	
}
