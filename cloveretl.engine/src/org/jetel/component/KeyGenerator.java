/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
 *  <h3>Key Generator Component</h3> 
 *  <!-- This component creates key which is costructed as combination of chars 
 * from given data fields.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>KeyGenerator</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 * Finds intersection of data flows (sets) <b>A (in-port0)</b> and <b>B (in-port1)</b> 
 * based on specified key. Both inputs <u><b>must be sorted</b></u> according to specified key. DataRecords only in flow <b>A</b>
 * are sent out through <b>out-port[0]</b>.
 * DataRecords in both <b>A&amp;B</b> are sent to specified <b>transformation</b> function and the result is 
 * sent through <b>out-port[1]</b>.
 * DataRecords present only in flow <b>B</b> are sent through <b>out-port[2]</b>.<br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - records from set A - <i>sorted according to specified key</i><br>
 *    [1] - records from set B - <i>sorted according to specified key</i><br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - records only in set A<br>
 *        [1] - records in set A&amp;B<br>
 *        [2] - records only in set B   
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"KEY_GEN"</td></tr>
 *    <tr><td><b>key</b></td><td>   </td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>can be used to specify different key field names for records on slave input; field names separated by :;|  {colon, semicolon, pipe}</td></tr>
 *    <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *      to search for class to be used for transforming data specified in <tt>transformClass<tt> parameter.</td></tr>
 *    <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td></tr>
 *    <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td></tr>
 *    <tr><td><b>javaSource</b></td><td>java source code implementation of transformation included direct into node definition</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="CustomerID" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *<pre>&lt;Node id="INTERSEC" type="DATA_INTERSECT" joinKey="EmployeeID"&gt;
 *&lt;attr name="javaSource"&gt;
 *import org.jetel.component.DataRecordTransform;
 *import org.jetel.data.*;
 * 
 *public class intersectionTest extends DataRecordTransform{
 *
 *  public boolean transform(DataRecord[] source, DataRecord[] target){
 *      
 *      target[0].getField(0).setValue(source[0].getField(0).getValue());
 *      target[0].getField(1).setValue(source[0].getField(1).getValue());
 *      target[0].getField(2).setValue(source[1].getField(2).getValue());
 *      return true;
 *  }
 *}
 *&lt;/attr&gt;
 *&lt;/Node&gt;</pre>
 *
 * @author avackova
 *
 */
public class KeyGenerator extends Node {

	public static final char SWITCH_REMOVE_DIACRITIC = 'd';
    public static final char SWITCH_ONLY_NUMERIC = 'n';
    public static final char SWITCH_ONLY_ALPHA = 'a';
    public static final char SWITCH_REMOVE_BLANKS = 's';
    public static final char SWITCH_UPPERCASE = 'u';
    public static final char SWITCH_LOWERCASE = 'l';

    private static final String XML_KEY_EXPRESSION_ATTRIBUTE = "keyExpression";

	public final static String COMPONENT_TYPE = "KEY_GEN";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private final static int LOWER = 0;
	private final static int UPPER = 1;
	private final static int ALPHA = 0;
	private final static int NUMERIC = 1; 

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
	private String fieldString=null;

	/**
	 * @param id
	 */
	public KeyGenerator(String id, String[] key) {
		super(id);
		this.key = key;
	}

	/**
	 * @param inMetadata 		metadata on input port
	 * @param outMetadata		metadata on output port
	 * @param fieldMap	int[in.getNumFields][2] - array in which are stored field's numbers from in and field's numbers on out
	 * @return number of field in out which does not exist in in
	 * @throws ComponentNotReadyException
	 */
	private int mapFields(DataRecordMetadata inMetadata,DataRecordMetadata outMetadata,
			int[][] fieldMap) throws ComponentNotReadyException{
		if (!(outMetadata.getNumFields()==inMetadata.getNumFields()+1))
			throw new ComponentNotReadyException("Metadata on output does not correspond with metadata on input!");
		int fieldNumber=0;
		int outMetadataIndex;
		for (outMetadataIndex=0;outMetadataIndex<outMetadata.getNumFields();outMetadataIndex++){
			int inMetadaIndex;
			for (inMetadaIndex=0;inMetadaIndex<inMetadata.getNumFields();inMetadaIndex++){
				if (outMetadata.getField(outMetadataIndex).getName().equals(inMetadata.getField(inMetadaIndex).getName())){
					fieldMap[outMetadataIndex][0]=inMetadaIndex;
					fieldMap[outMetadataIndex][1]=outMetadataIndex;
					break;
				}
			}
			if (inMetadaIndex==inMetadata.getNumFields())
				fieldNumber=inMetadaIndex;
		}
		return fieldNumber;
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
	private void fillOutRecord(DataRecord inRecord,DataRecord outRecord,
			int[][] map,int num,String value){
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
				fieldString=inRecord.getField(keys[i].getName()).getValue().toString();
				fieldString = StringUtils.getOnlyAlpfaNumericChars(fieldString,onlyAlpfaNumeric[i][ALPHA],onlyAlpfaNumeric[i][NUMERIC]);
				if (removeBlankSpace[i]) {
					fieldString = StringUtils.removeBlankSpace(fieldString);
				}
				if (removeDiacritic[i]){
					fieldString=StringUtils.removeDiacritic(fieldString);
				}
				if (lowerUpperCase[i][LOWER]){
					fieldString=fieldString.toLowerCase();
				}
				if (lowerUpperCase[i][UPPER]){
					fieldString=fieldString.toUpperCase();
				}
			}catch(NullPointerException ex){//value of field is null
				fieldString="";
			}
			if (fieldString.length()>=keys[i].getLenght()){
				if (keys[i].fromBegining){
					int start=keys[i].getStart();
					resultString.append(fieldString.substring(start,start+keys[i].getLenght()));
				}else{
					int end=fieldString.length();
					resultString.append(fieldString.substring(end-keys[i].getLenght(),end));
				}
			}else {
				//string from the field is shorter then demanded part of the key
				//get whole string from the field and add to it spaces
				StringBuffer newFieldString=new StringBuffer(fieldString);
				int offset;
				if (!keys[i].fromBegining) {
					offset=0;
				}else{
					offset=newFieldString.length();
				}
				for (int k=newFieldString.length();k<keys[i].getLenght();k++){
					newFieldString.insert(offset,' ');
				}
				resultString.append(newFieldString);
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
	 * 	onlyAlpfaNumeric[], removeDiacritic[] from the XML_KEY_EXPRESSION_ATTRIBUTE
	 *  for given part of the key
	 * 
	 * @param param - part of the key (from XML_KEY_EXPRESSION_ATTRIBUTE); 
	 * 			it is in form: name [from][-][number of letters][l|u][sand]
	 * @param i
	 */
	private void getParam(String param,int i){
		String[] paramWords=param.split(" ");
		String keyParam=paramWords[1];
		int start=-1;
		int lenght=0;
		boolean fromBegining=true;
		StringBuffer number=new StringBuffer();
		int counter=0;
		char charFromKeyParam=' ';
		number.setLength(0);
		while (counter<keyParam.length() && Character.isDigit(charFromKeyParam=keyParam.charAt(counter))){
			number.append(charFromKeyParam);
			counter++;
		}
		if (charFromKeyParam=='-') {
			if (counter>0){
				start=Integer.parseInt(number.toString())-1;
			}else {
				fromBegining=false;
			}
			counter++;
		}else{
			lenght=Integer.parseInt(number.toString());
		}
		number.setLength(0);
		while (counter<keyParam.length() && Character.isDigit(charFromKeyParam=keyParam.charAt(counter))){
			number.append(charFromKeyParam);
			counter++;
		}
		if (number.length()>0) {
			lenght=Integer.parseInt(number.toString());
		}
		if (start>-1){
			keys[i] = new Key(paramWords[0],start,lenght);
		}else{
			keys[i] = new Key(paramWords[0],lenght,fromBegining);
		}
		while (counter<keyParam.length()){
			charFromKeyParam=Character.toLowerCase(keyParam.charAt(counter));
			switch (charFromKeyParam) {
			case SWITCH_LOWERCASE:lowerUpperCase[i][LOWER] = true;
				break;
			case SWITCH_UPPERCASE:lowerUpperCase[i][UPPER] = true;
				break;
			case SWITCH_REMOVE_BLANKS:removeBlankSpace[i] = true;
				break;
			case SWITCH_ONLY_ALPHA:onlyAlpfaNumeric[i][ALPHA] = true;
				break;
			case SWITCH_ONLY_NUMERIC:onlyAlpfaNumeric[i][NUMERIC] = true;
				break;
			case SWITCH_REMOVE_DIACRITIC:removeDiacritic[i] = true;
			}
			counter++;
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
		xmlElement.setAttribute(XML_KEY_EXPRESSION_ATTRIBUTE,StringUtils.stringArraytoString(key,Defaults.Component.KEY_FIELDS_DELIMITER.charAt(0)));
	}
	

	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		try {
			return new KeyGenerator(Node.XML_ID_ATTRIBUTE,xattribs.getString(XML_KEY_EXPRESSION_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
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
