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

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
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
 * This component creates key which is constructed as combination of chars 
 * from given data fields.It is possible to make some operation on original strings
 * as removing blank space, removing diacritic, getting only alpha signs, etc. <br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0]- input records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - record as on input port, but with additional field with generated 
 *        		key
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
 *    <tr><td><b>keyExpression</b></td><td> field names with the way of generating
 *    	 the key from them, separated by :;|  {colon, semicolon, pipe}. String,
 *    	 which describes creating of key for each field has form: 
 *    	 [from][-]how many[d|n|a|s|u|l]. When there is no "from" it means key
 *    	 is created from chars from the beginning of string or if there is 
 *    	 "-from" the end of string<br>Parameters:<br>
 *    	d - remove diacritic (change the diacritic letters to theirs latin equivalents)<br>
 *    	n - get only numeric signs (checking Character.isDigit)<br>
 *      a - get only alpha signs (checking Character.isLetter)<br>
 *      s - remove blank space (checking Character.isWhitespace)<br>
 *      u - change to upper case<br>
 *      l - change to lower case</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="KEY_GEN0" type="KEY_GEN"&gt;
 *&lt;attr name="keyExpression">lname 2d;fname -2ad&lt;/attr&gt;
 *&lt;/Node&gt;</pre>
 *
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
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
				if (fieldString == null){
					fieldString ="";
				}
				fieldString = StringUtils.getOnlyAlphaNumericChars(fieldString,onlyAlpfaNumeric[i][ALPHA],onlyAlpfaNumeric[i][NUMERIC]);
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
	
	@Override
	public Result execute() throws Exception {
		DataRecord inRecord = DataRecordFactory.newRecord(inMetadata);
		inRecord.init();
		DataRecord outRecord = DataRecordFactory.newRecord(outMetadata);
		outRecord.init();
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);// readRecord(READ_FROM_PORT,inRecord);
			if (inRecord != null) {
				fillOutRecord(inRecord, outRecord, fieldMap, outKey,
						generateKey(inRecord, keys));
				outPort.writeRecord(outRecord);
				SynchronizeUtils.cloverYield();
			}
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
		
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
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
	
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		return new KeyGenerator(xattribs.getString(XML_ID_ATTRIBUTE),xattribs.getString(XML_KEY_EXPRESSION_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, 1)) {
        	return status;
        }

        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
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
