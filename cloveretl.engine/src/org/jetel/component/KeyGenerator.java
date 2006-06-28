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
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 * This component creates key which is costructed as combination of chars 
 * from given data fields
 * @author avackova
 *
 */
public class KeyGenerator extends Node {

	private static final String XML_KEY_ATTRIBUTE = "key";

	public final static String COMPONENT_TYPE = "KEY_GEN";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private final static int LOWER = 0;
	private final static int UPPER = 1;
	private final static int ALPHA = 0;
	private final static int NUMRIC = 1; 

	private String[] key;
	private Key[] keys;
	private boolean[][] lowerUpperCase;
	private boolean[] removeBlankSpace;
	private boolean[][] onlyAlpfaNumeric;
	private boolean[] removeDiacritic;
	private int[][] fieldMap;
	private int outKey;
	private InputPort inPort;
	private DataRecordMetadata inMetadata;
	private OutputPort outPort;
	private DataRecordMetadata outMetadata;
	
	private int lenght=0;
	private StringBuffer resultString;	
	private String pom=null;

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
	
	/**
	 * This method fills outRecord with the data from inRecord. 
	 * outRecord has one more field then inRecord; to this field is 
	 * set given value 
	 * 
	 * @param inRecord
	 * @param outRecord
	 * @param map - in map[0] are numbers of fields in inRecord 
	 * 				 and in map[1] are corresponding fields in outRecord
	 * @param num - number of field in outRecord, which is lacking in inRecord
	 * @param value - value to be set to field number "num"
	 */
	private void fillOutRecord(DataRecord inRecord,DataRecord outRecord,int[][] map,int num,String value){
		for (int i=0;i<map.length;i++){
			outRecord.getField(map[i][1]).setValue(inRecord.getField(map[i][0]).getValue());
		}
		outRecord.getField(num).setValue(value);
	}
	
	/**
	 * This method generates refernece key for input record
	 * 
	 * @param inRecord
	 * @param key - fields from which the key is to be constructed
	 * @return key for input record
	 */
	private String generateKey(DataRecord inRecord, Key[] key){
		resultString.setLength(0);
		for (int i=0;i<keys.length;i++){
			try{ //get field value from inRcord
				pom=inRecord.getField(keys[i].getName()).getValue().toString();
				pom = StringUtils.getOnlyAlpfaNumericChars(pom,onlyAlpfaNumeric[i][ALPHA],onlyAlpfaNumeric[i][NUMRIC]);
				if (removeBlankSpace[i]) {
					pom = StringUtils.removeBlankSpace(pom);
				}
				if (removeDiacritic[i]){
					pom=StringUtils.removeDiacritic(pom);
				}
				if (lowerUpperCase[i][LOWER]){
					pom=pom.toLowerCase();
				}
				if (lowerUpperCase[i][UPPER]){
					pom=pom.toUpperCase();
				}
			}catch(NullPointerException ex){//value of field is null
				pom="";
			}
			if (pom.length()>=keys[i].getLenght()){
				if (keys[i].fromBegining){
					int start=keys[i].getStart();
					resultString.append(pom.substring(start,start+keys[i].getLenght()));
				}else{
					int end=pom.length();
					resultString.append(pom.substring(end-keys[i].getLenght(),end));
				}
			}else {
				//string from the field is shorter then demanded part of the key
				//get whole string from the field and add to it spaces
				StringBuffer shortPom=new StringBuffer(pom);
				int offset;
				if (!keys[i].fromBegining) {
					offset=0;
				}else{
					offset=shortPom.length();
				}
				for (int k=shortPom.length();k<keys[i].getLenght();k++){
					shortPom.insert(offset,' ');
				}
				resultString.append(shortPom);
			}
		}
		return resultString.toString();
	}
	
	/**
	 *  Main processing method for the KeyGerator object
	 */
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

	/**
	 * This method fills keys[], lowerUpperCase[], removeBlankSpace[],
	 * 	onlyAlpfaNumeric[], removeDiacritic[] from the XML_KEY_ATTRIBUTE
	 *  for given part of the key
	 * 
	 * @param param - part of the key (from XML_KEY_ATTRIBUTE); 
	 * 			it is in form: name [from][-][number of letters][l|u][sand]
	 * @param i
	 */
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
			case 'l':lowerUpperCase[i][LOWER] = true;
				break;
			case 'u':lowerUpperCase[i][UPPER] = true;
				break;
			case 's':removeBlankSpace[i] = true;
				break;
			case 'a':onlyAlpfaNumeric[i][ALPHA] = true;
				break;
			case 'n':onlyAlpfaNumeric[i][NUMRIC] = true;
				break;
			case 'd':removeDiacritic[i] = true;
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
		outKey=mapFields(inMetadata,outMetadata,fieldMap);//number of field which is in out metadata, but is lacking in in metadata
		int length=key.length;
		keys=new Key[length];
		lowerUpperCase = new boolean[length][2];
		removeBlankSpace = new boolean[length];
		onlyAlpfaNumeric = new boolean[length][2];
		removeDiacritic = new boolean[length];
		//filling keys, lowerUpperCase, removeBlankSpace, onlyAlpfaNumeric, removeDiacritic
		for (int i=0;i<length;i++){
			getParam(key[i],i);
		}
		length = 0;
		for (int i=0;i<keys.length;i++){
			lenght+=keys[i].getLenght();
		}
		resultString=new StringBuffer(lenght);
	}
	
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_KEY_ATTRIBUTE,StringUtils.stringArraytoString(key,Defaults.Component.KEY_FIELDS_DELIMITER.charAt(0)));
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
