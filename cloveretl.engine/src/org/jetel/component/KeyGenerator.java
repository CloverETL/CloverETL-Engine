/**
 * 
 */
package org.jetel.component;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;

/**
 * This component creates reference matching key which is 
 * costructed as combination of signs from given data fields
 * @author avackova
 *
 */
public class KeyGenerator extends Node {

	private static final String XML_KEY_ATTRIBUTE = "key";
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "KEY_GEN";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private Key[] keys;
	private int[][] fieldMap;
	private int outKey;
	private InputPort inPort;
	private DataRecordMetadata inMetadata;
	private OutputPort outPort;
	private DataRecordMetadata outMetadata;
	
	/**
	 * @param id
	 */
	public KeyGenerator(String id, Key[] keys) {
		super(id);
		this.keys = keys;
	}

	/**
	 * @param in 		metadata on input port
	 * @param out		metadata on output port
	 * @param fieldMap	int[in.getNumFields][2] - array in which are stored field's numbers from in and field's numbers on out
	 * @return number of field in out which does not exist in in
	 * @throws ComponentNotReadyException
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
		for (int i=0;i<keys.length;i++){
			j+=keys[i].getEnd()-keys[i].getStart();
		}
		StringBuffer resultString=new StringBuffer(j);
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);// readRecord(READ_FROM_PORT,inRecord);
				if (inRecord!=null) {
					resultString.setLength(0);
					for (int i=0;i<keys.length;i++){
						String pom;
						try{
							pom=inRecord.getField(keys[i].getName()).getValue().toString();
						}catch(NullPointerException ex){
							pom="";
						}
						try {
							resultString.append(pom.substring(keys[i].getStart(),keys[i].getEnd()));
						}catch (StringIndexOutOfBoundsException ex){
							for (int k=0;k<keys[i].getLength();k++){
								if (pom.length()>k)
									resultString.append(pom.charAt(k));
								else
									resultString.append(' ');
							}
						}
					}
					fillOutRecord(inRecord,outRecord,fieldMap,outKey,resultString.toString());
					outPort.writeRecord(outRecord);
					SynchronizeUtils.cloverYield();
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
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}

	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and exactly one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() != 1) {
			throw new ComponentNotReadyException("One output port has to be defined!");
		}
//		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
//		if (recordBuffer == null) {
//			throw new ComponentNotReadyException("Can NOT allocate internal record buffer ! Required size:" +
//					Defaults.Record.MAX_RECORD_SIZE);
//		}
		inPort = getInputPort(READ_FROM_PORT);
		inMetadata=inPort.getMetadata();
		outPort = getOutputPort(WRITE_TO_PORT);
		outMetadata=outPort.getMetadata();
		fieldMap=new int[inMetadata.getNumFields()][2];
		outKey=mapFields(inMetadata,outMetadata,fieldMap);
	}

	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		KeyGenerator result;		
		try {
			String[] keys=xattribs.getString(XML_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			Key[] sortKeys=new Key[keys.length];
			for (int i=0;i<keys.length;i++){
				String[] pom=keys[i].split(" ");
				switch (pom.length) {
					case 2:sortKeys[i]=new Key(pom[0],Integer.parseInt(pom[1]));
						break;
					case 3:sortKeys[i]=new Key(pom[0],Integer.parseInt(pom[1]),Integer.parseInt(pom[2]));
						break;
					default:	System.err.println(COMPONENT_TYPE + ":wrong format of XML_KEY_ATTRIBUTE" );
						return null;
				}
			}
			result = new KeyGenerator(Node.XML_ID_ATTRIBUTE,sortKeys);
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
		
		public int getLength(){
			return end-start;
		}
	}
	
}
