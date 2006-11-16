
package org.jetel.component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;
import org.jetel.util.WcardPattern;

public class CustomizedRecordTransform implements RecordTransform {
	
	protected Properties parameters;
	protected DataRecordMetadata[] sourceMetadata;
	protected DataRecordMetadata[] targetMetadata;

	protected TransformationGraph graph;
	
	protected Map<String, String> rules = new LinkedHashMap<String, String>();
	protected Rule[][] transformMapArray;
	protected int[][] order;
	
	protected static final int REC_NO = 0;
	protected static final int FIELD_NO = 1;
	
	protected static final char DOT = '.';
	protected static final char COLON =':';
	protected static final char PARAMETER_CHAR = '$'; 
	
	private	 int ruleType;
	private String ruleString;
	private String sequenceID;


	public void addFieldToFieldRule(String patternOut, String patternIn) {
		String outField = resolveField(patternOut);
		String inField = resolveField(patternIn);
		if (outField != null && inField != null) {
			rules.put(outField, String.valueOf(Rule.FIELD) + COLON + inField);
		}else{
//			throw new TransformException("Wrong field mask");
		}
	}

	public void addFieldToFieldRule(int recNo, int fieldNo, String patternIn){
		addFieldToFieldRule(String.valueOf(recNo) + DOT + fieldNo, patternIn);
	}
	
	public void addFieldToFieldRule(int recNo, String field, String patternIn){
		addFieldToFieldRule(String.valueOf(recNo) + DOT + field, patternIn);
	}
	
	public void addFieldToFieldRule(int fieldNo, String patternIn){
		addFieldToFieldRule(0, fieldNo, patternIn);
	}
	
	public void addFieldToFieldRule(String patternOut, int recNo, int fieldNo){
		addFieldToFieldRule(patternOut, String.valueOf(recNo) + DOT + fieldNo);
	}
	
	public void addFieldToFieldRule(String patternOut, int recNo, String field){
		addFieldToFieldRule(patternOut, recNo + DOT + field);
	}
	
	public void addFieldToFieldRule(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo){
		addFieldToFieldRule(String.valueOf(outRecNo) + DOT + outFieldNo,
				String.valueOf(inRecNo) + DOT + inFieldNo);
	}
	
	public void addConstantToFieldRule(String patternOut, String value){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.CONSTANT) + COLON + value);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addConstantToFieldRule(String patternOut, int value){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.CONSTANT) + COLON + value);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addConstantToFieldRule(String patternOut, double value){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field,String.valueOf(Rule.CONSTANT) + COLON + value);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}

	public void addConstantToFieldRule(String patternOut, Date value){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.CONSTANT) + COLON + 
					SimpleDateFormat.getDateInstance().format(value));
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}

	public void addConstantToFieldRule(String patternOut, Numeric value){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.CONSTANT) + COLON + value);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}

	public void addConstantToFieldRule(int recNo, int fieldNo, String value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	public void addConstantToFieldRule(int recNo, int fieldNo, int value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	public void addConstantToFieldRule(int recNo, int fieldNo, double value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}
	
	public void addConstantToFieldRule(int recNo, int fieldNo, Date value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	public void addConstantToFieldRule(int recNo, int fieldNo, Numeric value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	public void addConstantToFieldRule(int recNo, String field, String value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	public void addConstantToFieldRule(int recNo, String field, int value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	public void addConstantToFieldRule(int recNo, String field, double value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	public void addConstantToFieldRule(int recNo, String field, Date value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	public void addConstantToFieldRule(int recNo, String field, Numeric value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	public void addConstantToFieldRule(int fieldNo, String value){
		addConstantToFieldRule(0, fieldNo, value);
	}

	public void addConstantToFieldRule(int fieldNo, int value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	public void addConstantToFieldRule(int fieldNo, double value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	public void addConstantToFieldRule(int fieldNo, Date value){
		addConstantToFieldRule(0, fieldNo, value);
	}

	public void addConstantToFieldRule(int fieldNo, Numeric value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}
	
	public void addSequenceToFieldRule(String patternOut, String sequence){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.SEQUENCE) + COLON + sequence);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addSequenceToFieldRule(int recNo, int fieldNo, String sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence);
	}
	
	public void addSequenceToFieldRule(int recNo, String field, String sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence);
	}
	
	public void addSequenceToFieldRule(int fieldNo, String sequence){
		addSequenceToFieldRule(0,fieldNo, sequence);
	}
	
	public void addSequenceToFieldRule(String patternOut, Sequence sequence){
		addSequenceToFieldRule(patternOut, sequence.getId());
	}
	
	public void addSequenceToFieldRule(int recNo, int fieldNo, Sequence sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence.getId());
	}
	
	public void addSequenceToFieldRule(int recNo, String field, Sequence sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence.getId());
	}
	
	public void addSequenceToFieldRule(int fieldNo, Sequence sequence){
		addSequenceToFieldRule(0,fieldNo, sequence.getId());
	}

	public void addParameterToFieldRule(String patternOut, String parameterName){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, String.valueOf(Rule.PARAMETER) + COLON + parameterName);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}

	public void addParameterToFieldRule(int recNo, int fieldNo, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + fieldNo, parameterName);
	}
	
	public void addParameterToFieldRule(int recNo, String field, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + field, parameterName);
	}
	
	public void addParameterToFieldRule(int fieldNo, String parameterName){
		addParameterToFieldRule(0,fieldNo, parameterName);
	}
	
	public void finished() {
		// TODO Auto-generated method stub

	}

	public TransformationGraph getGraph() {
		return graph;
	}

	public String getMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getSemiResult() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean init(Properties parameters, DataRecordMetadata[] sourcesMetadata,
			DataRecordMetadata[] targetMetadata) throws ComponentNotReadyException {
		if (sourcesMetadata == null || targetMetadata == null)
			return false;
		this.parameters=parameters;
		this.sourceMetadata=sourcesMetadata;
		this.targetMetadata=targetMetadata;
	    return init();
	}

	private boolean init() throws ComponentNotReadyException{
		Map<String, Rule> transformMap = new LinkedHashMap<String, Rule>();
		Entry<String, String> rulesEntry;
		Rule rule;
		int type;
		String field;
		String ruleString;
		String[] outFields = new String[0];
		String[] inFields;
		int[] recFieldNo = new int[2];
		for (Iterator<Entry<String, String>> iterator = rules.entrySet().iterator();iterator.hasNext();){
			rulesEntry = iterator.next();
			outFields = findFields(rulesEntry.getKey(), targetMetadata).toArray(new String[0]);
			inFields = new String[0];
			type = Integer.parseInt(rulesEntry.getValue().substring(0, rulesEntry.getValue().indexOf(COLON)));
			ruleString = rulesEntry.getValue().substring(rulesEntry.getValue().indexOf(COLON)+1);
			if (type == Rule.FIELD) {
				inFields = findFields(ruleString, sourceMetadata).toArray(new String[0]);
			}
			if (type == Rule.FIELD && inFields.length > 1){
				putMappingByNames(transformMap,outFields,inFields);
			}else{
				for (int i=0;i<outFields.length;i++){
					field = outFields[i];
					recFieldNo[REC_NO] = Integer.valueOf(field.substring(0, field.indexOf(DOT)));
					recFieldNo[FIELD_NO] = Integer.valueOf(field.substring(field.indexOf(DOT)+1));
					rule = transformMap.remove(String.valueOf(recFieldNo[REC_NO]) + DOT + recFieldNo[FIELD_NO]);
					if (rule == null) {
						rule = new Rule(type,null);
					}else{
						rule.setType(type);
					}
					if (type != Rule.FIELD) {
						rule.setValue(ruleString);
					}else{
						rule.setValue(inFields[0]);
					}
					transformMap.put(String.valueOf(recFieldNo[REC_NO]) + DOT + recFieldNo[FIELD_NO], rule);
				}
			}
		}
		transformMapArray = new Rule[targetMetadata.length][maxNumFields(targetMetadata)];
		order = new int[transformMap.size()][2];
		int index = 0;
		for (Entry<String, Rule> i : transformMap.entrySet()) {
			field = i.getKey();
			order[index][REC_NO] = Integer.valueOf(field.substring(0, field.indexOf(DOT)));
			order[index][FIELD_NO] = Integer.valueOf(field.substring(field.indexOf(DOT)+1));
			transformMapArray[order[index][REC_NO] ][order[index][FIELD_NO]] = i.getValue();
			index++;
		}
		return true;
	}
	
	private void putMappingByNames(Map<String, Rule> transformMap, 
			String[] outFields, String[] inFields){
		String[][] outFieldsName = new String[targetMetadata.length][maxNumFields(targetMetadata)];
		int recNo;
		int fieldNo;
		for (int i = 0; i < outFields.length; i++) {
			recNo = Integer.valueOf(outFields[i].substring(0, outFields[i].indexOf(DOT)));
			fieldNo = Integer.valueOf(outFields[i].substring(outFields[i].indexOf(DOT)+1));
			outFieldsName[recNo][fieldNo] = targetMetadata[recNo].getField(fieldNo).getName();
		}
		String[][] inFieldsName = new String[sourceMetadata.length][maxNumFields(sourceMetadata)];
		for (int i = 0; i < inFields.length; i++) {
			recNo = Integer.valueOf(inFields[i].substring(0, inFields[i].indexOf(DOT)));
			fieldNo = Integer.valueOf(inFields[i].substring(inFields[i].indexOf(DOT)+1));
			inFieldsName[recNo][fieldNo] = sourceMetadata[recNo].getField(fieldNo).getName();
		}
		int index;
		Rule rule;
		//find identical in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findString(outFieldsName[i][j],
							inFieldsName[i]);
					if (index > -1) {
						rule = transformMap.remove(String.valueOf(i) + DOT + j);
						if (rule == null) {
							rule = new Rule(Rule.FIELD, String.valueOf(i) + DOT
									+ index);
						} else {
							rule.setType(Rule.FIELD);
							rule.setValue(String.valueOf(i) + DOT + index);
						}
						transformMap.put(String.valueOf(i) + DOT + j, rule);
						outFieldsName[i][j] = null;
						inFieldsName[i][index] = null;
					}
				}				
			}
		}
		//find ignore case in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findStringIgnoreCase(
							outFieldsName[i][j], inFieldsName[i]);
					if (index > -1) {
						rule = transformMap.remove(String.valueOf(i) + DOT + j);
						if (rule == null) {
							rule = new Rule(Rule.FIELD, String.valueOf(i) + DOT
									+ index);
						} else {
							rule.setType(Rule.FIELD);
							rule.setValue(String.valueOf(i) + DOT + index);
						}
						transformMap.put(String.valueOf(i) + DOT + j, rule);
						outFieldsName[i][j] = null;
						inFieldsName[i][index] = null;
					}
				}				
			}
		}
		//find identical in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findString(outFieldsName[i][j],
								inFieldsName[k]);
						if (index > -1) {
							rule = transformMap.remove(String.valueOf(i) + DOT
									+ j);
							if (rule == null) {
								rule = new Rule(Rule.FIELD, String.valueOf(k)
										+ DOT + index);
							} else {
								rule.setType(Rule.FIELD);
								rule.setValue(String.valueOf(k) + DOT + index);
							}
							transformMap.put(String.valueOf(i) + DOT + j, rule);
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
							break;
						}
					}					
				}
			}
		}
		//find ignore case in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findStringIgnoreCase(
								outFieldsName[i][j], inFieldsName[k]);
						if (index > -1) {
							rule = transformMap.remove(String.valueOf(i) + DOT
									+ j);
							if (rule == null) {
								rule = new Rule(Rule.FIELD, String.valueOf(k)
										+ DOT + index);
							} else {
								rule.setType(Rule.FIELD);
								rule.setValue(String.valueOf(k) + DOT + index);
							}
							transformMap.put(String.valueOf(i) + DOT + j, rule);
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
							break;
						}
					}					
				}
			}
		}
	}
	
	private ArrayList<String> findFields(String pattern,DataRecordMetadata[] metadata){
		ArrayList<String> list = new ArrayList<String>();
		String recordNoString = pattern.substring(0,pattern.indexOf(DOT));
		String fieldNoString = pattern.substring(pattern.indexOf(DOT)+1);
		int fieldNo;
		int recNo;
		try {
			recNo = Integer.parseInt(recordNoString);
			try {
				fieldNo = Integer.parseInt(fieldNoString);
				list.add(recordNoString + DOT + fieldNoString);
			}catch(NumberFormatException e){
				for (int i=0;i<metadata[recNo].getNumFields();i++){
					if (WcardPattern.checkName(fieldNoString, metadata[recNo].getField(i).getName())){
						list.add(String.valueOf(recNo) + DOT + i);
					}
				}
			}
		}catch (NumberFormatException e){
			for (int i=0;i<metadata.length;i++){
				if (WcardPattern.checkName(recordNoString, metadata[i].getName()))
					try {
						fieldNo = Integer.parseInt(fieldNoString);
						list.add(String.valueOf(i) + DOT + fieldNoString);
					}catch(NumberFormatException e1){
						for (int j=0;j<metadata[i].getNumFields();j++){
							if (WcardPattern.checkName(fieldNoString, metadata[i].getField(j).getName())){
								list.add(String.valueOf(i) + DOT + j);
							}
						}
					}
				}
			}
		return list;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void signal(Object signalObject) {
		// TODO Auto-generated method stub

	}

	public boolean transform(DataRecord[] sources, DataRecord[] target)
			throws TransformException {
		for (int i = 0; i < order.length; i++) {
			ruleType = transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getType();
			ruleString = transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue();
			switch (ruleType) {
			case Rule.FIELD:
				target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
						transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue(sources));
				break;
			case Rule.SEQUENCE:
				sequenceID = ruleString.indexOf(DOT) == -1 ? ruleString
						: ruleString.substring(0, ruleString.indexOf(DOT));
				target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
						transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue(getGraph()
								.getSequence(sequenceID)));
				break;
			case Rule.PARAMETER:
				if (ruleString.startsWith("${")){
					target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(
							getGraph().getGraphProperties().getProperty(
									ruleString.substring(2, ruleString.lastIndexOf('}'))));
				}else if (ruleString.startsWith(String.valueOf(PARAMETER_CHAR))){
					target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(
						parameters.getProperty((ruleString)));
				}else{
					String parameterValue = parameters.getProperty(PARAMETER_CHAR + ruleString);
					if (parameterValue == null ){
						parameterValue = getGraph().getGraphProperties().getProperty(ruleString);
					}
					target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(
							parameterValue);
				}
				break;
			default:// constant
				if (target[order[i][REC_NO]].getField(order[i][FIELD_NO]).getType() == DataFieldMetadata.DATE_FIELD
						|| target[order[i][REC_NO]].getField(order[i][FIELD_NO]).getType() == DataFieldMetadata.DATETIME_FIELD) {
					try {
						Date date = SimpleDateFormat.getDateInstance()
								.parse(ruleString);
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(date);
					} catch (ParseException e) {
						// value was set as String not a Date
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(ruleString);
					}
				} else {
					target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(ruleString);
				}
				break;
			}
		}
		return true;
	}
	
	private static String resolveField(String pattern){
		String[] parts = pattern.split("\\.");
		switch (parts.length) {
		case 2:
			if (parts[0].startsWith("$")){// ${recNo.field}
				return parts[0].substring(2) + DOT + parts[1].substring(0,parts[1].length()-1);
			}else{// recNo.field
				return pattern;
			}
		case 3:
			if (parts[0].startsWith("$")){// ${out.recNo.field}
				return parts[1] + DOT + parts[2].substring(0,parts[2].length() -1);
			}else{//out.recNo.field
				return parts[1] + DOT + parts[2];
			}
		default:return null;
		}
	}

	public Map<String, String> getRules() {
		return rules;
	}

	public ArrayList<String> getResolvedRules() {
		ArrayList<String> list = new ArrayList<String>();
		for (int recNo = 0;recNo < transformMapArray.length; recNo++){
			for (int fieldNo=0;fieldNo < transformMapArray[0].length; fieldNo++){
				if (transformMapArray[recNo][fieldNo] != null) {
					list.add("out" + DOT + String.valueOf(recNo) + DOT + fieldNo + "="
							+ transformMapArray[recNo][fieldNo].getValue());
				}				
			}
		}
		return list;
	}

	private int maxNumFields(DataRecordMetadata[] metadata){
		int numFields = 0;
		for (int i = 0; i < metadata.length; i++) {
			if (metadata[i].getNumFields() > numFields) {
				numFields = metadata[i].getNumFields();
			}
		}
		return numFields;
	}
	
	class Rule {
		
		final static int FIELD = 0;
		final static int CONSTANT = 1;
		final static int SEQUENCE = 2;
		final static int PARAMETER = 3;
		
		int type;
		String value;
		
		Rule(int type, String value){
			this.type = type;
			this.value = value;
		}
		
		String getValue(){
			return value;
		}
		
		void setValue(String value){
			this.value = value;
		}
		
		int getType() {
			return type;
		}

		void setType(int type) {
			this.type = type;
		}

		Object getValue(DataRecord[] records){
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			int recNo = dotIndex > -1 ? Integer.parseInt(value.substring(0, dotIndex)) : 0;
			int fieldNo = dotIndex > -1 ? Integer.parseInt(value.substring(dotIndex + 1)) : Integer.parseInt(value); 
			return records[recNo].getField(fieldNo).getValue();
		}
		
		Object getValue(Sequence sequence){
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			String method = dotIndex > -1 ? value.substring(dotIndex +1) : "nextValueInt()";
			if (method.equals("currentValueString()")){
				return sequence.currentValueString();
			}
			if (method.equals("nextValueString()")){
				return sequence.nextValueString();
			}
			if (method.equals("currentValueInt()")){
				return sequence.currentValueInt();
			}
			if (method.equals("nextValueInt()")){
				return sequence.nextValueInt();
			}
			if (method.equals("currentValueLong()")){
				return sequence.currentValueLong();
			}
			if (method.equals("nextValueLong()")){
				return sequence.nextValueLong();
			}
			return value;
		}
		
	}

}
