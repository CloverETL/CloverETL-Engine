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
	
	private final static int LOWER = 0;
	private final static int UPPER = 1;

	private String[] key;
	private Key[] keys;
	private boolean[][] lowerUpperCase;
	private boolean[] removeBlankSpace;
	private int[][] fieldMap;
	private int outKey;
	private InputPort inPort;
	private DataRecordMetadata inMetadata;
	private OutputPort outPort;
	private DataRecordMetadata outMetadata;
	
	int lenght=0;
	StringBuffer resultString;	
	String pom=null;
	StringBuffer toRemBlankSpace = new StringBuffer();
	StringBuffer afterRemBlankSpace = new StringBuffer();
	char[] pomChars;

	/**
	 * @param id
	 */
	public KeyGenerator(String id, String[] key) {
		super(id);
		this.key = key;
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
	
	private String generateKey(DataRecord inRecord, Key[] key){
		resultString.setLength(0);
		for (int i=0;i<keys.length;i++){
			try{
				toRemBlankSpace.setLength(0);
				toRemBlankSpace.append(inRecord.getField(keys[i].getName()).getValue().toString());
				if (removeBlankSpace[i]) {
					if (pomChars==null || pomChars.length<toRemBlankSpace.length()) {
						pomChars=new char[toRemBlankSpace.length()];
					}
					afterRemBlankSpace.setLength(0);
					toRemBlankSpace.getChars(0,toRemBlankSpace.length(),pomChars,0);
					for (int j=0;j<toRemBlankSpace.length();j++){
						if (!Character.isWhitespace(pomChars[j])) {
							afterRemBlankSpace.append(pomChars[j]);
						}
					}
					pom=afterRemBlankSpace.toString();
				}else{
					pom=toRemBlankSpace.toString();
				}
				if (lowerUpperCase[i][LOWER]){
					pom=pom.toLowerCase();
				}
				if (lowerUpperCase[i][UPPER]){
					pom=pom.toUpperCase();
				}
			}catch(NullPointerException ex){
				pom="";
			}
			try {
				if (keys[i].fromBegining){
					int start=keys[i].getStart();
					resultString.append(pom.substring(start,start+keys[i].getLenght()));
				}else{
					int end=pom.length();
					resultString.append(pom.substring(end-keys[i].getLenght(),end));
				}
			}catch (StringIndexOutOfBoundsException ex){
				StringBuffer shortPom=new StringBuffer(keys[i].getLenght());
				//when keys[i].fromBegining=false there is k=-1
				for (int k=keys[i].getStart();k<keys[i].getStart()+pom.length();k++){
					if (pom.length()>k){
						if (keys[i].fromBegining){
							shortPom.append(pom.charAt(k));
						}else{
							shortPom.insert(0,pom.charAt(pom.length()-2-k));
						}
					}
				}
				int offset;
				if (!keys[i].fromBegining) {
					offset=0;
				}else{
					offset=shortPom.length();
				}
				for (int k=shortPom.length();k<keys[i].lenght;k++){
					shortPom.insert(offset,' ');
				}
				resultString.append(shortPom);
			}
		}
		return resultString.toString();
	}
	
	public void run() {
		DataRecord inRecord = new DataRecord(inMetadata);
		inRecord.init();
		DataRecord outRecord = new DataRecord(outMetadata);
		outRecord.init();
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);// readRecord(READ_FROM_PORT,inRecord);
				if (inRecord!=null) {
					fillOutRecord(inRecord,outRecord,fieldMap,outKey,generateKey(inRecord,keys));
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
				closeAllOutputPorts();
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

	private void getParam(String param,int i){
		String[] pom=param.split(" ");
		String keyParam=pom[1];
		int start=-1;
		int lenght=0;
		boolean fromBegining=true;
		StringBuffer number=new StringBuffer();
		int j=0;
		char c=' ';
		number.setLength(0);
		while (j<keyParam.length() && Character.isDigit(c=keyParam.charAt(j))){
			number.append(c);
			j++;
		}
		if (c=='-') {
			if (j>0){
				start=Integer.parseInt(number.toString())-1;
			}else {
				fromBegining=false;
			}
			j++;
		}else{
			lenght=Integer.parseInt(number.toString());
		}
		number.setLength(0);
		while (j<keyParam.length() && Character.isDigit(c=keyParam.charAt(j))){
			number.append(c);
			j++;
		}
		if (number.length()>0) {
			lenght=Integer.parseInt(number.toString());
		}
		if (start>-1){
			keys[i] = new Key(pom[0],start,lenght);
		}else{
			keys[i] = new Key(pom[0],lenght,fromBegining);
		}
		while (j<keyParam.length()){
			c=Character.toLowerCase(keyParam.charAt(j));
			switch (c) {
			case 'l':lowerUpperCase[i][LOWER]=true;
				break;
			case 'u':lowerUpperCase[i][UPPER]=true;
				break;
			case 's':removeBlankSpace[i] = true;
				break;
			}
			j++;
		}
	}
		
	public void init() throws ComponentNotReadyException {
		// test that we have one input port and exactly one output
		if (inPorts.size() != 1) {
			throw new ComponentNotReadyException("One input port has to be defined!");
		} else if (outPorts.size() != 1) {
			throw new ComponentNotReadyException("One output port has to be defined!");
		}
		inPort = getInputPort(READ_FROM_PORT);
		inMetadata=inPort.getMetadata();
		outPort = getOutputPort(WRITE_TO_PORT);
		outMetadata=outPort.getMetadata();
		fieldMap=new int[inMetadata.getNumFields()][2];
		outKey=mapFields(inMetadata,outMetadata,fieldMap);
		int length=key.length;
		keys=new Key[length];
		lowerUpperCase = new boolean[length][2];
		removeBlankSpace = new boolean[length];
		for (int i=0;i<length;i++){
			getParam(key[i],i);
		}
		for (int i=0;i<keys.length;i++){
			lenght+=keys[i].getLenght();
		}
		resultString=new StringBuffer(lenght);
	}

	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		try {
			return new KeyGenerator(Node.XML_ID_ATTRIBUTE,xattribs.getString(XML_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
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
		int lenght;
		boolean fromBegining;
		
		Key(String name,int start,int length){
			this.name=name;
			this.start=start;
			this.lenght=length;
			fromBegining=true;
		}
		
		Key(String name,int length,boolean fromBegining){
			this.name=name;
			this.fromBegining=fromBegining;
			this.lenght=length;
			if (fromBegining) {
				this.start=0;
			}else{
				this.start=-1;
			}
		}

		public int getLenght() {
			return lenght;
		}

		public String getName() {
			return name;
		}

		public int getStart() {
			return start;
		}
		
	}
	
}
