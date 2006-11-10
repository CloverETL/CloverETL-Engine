
package org.jetel.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;

public class CustomizedRecordTransform implements RecordTransform {
	
	private Properties parameters;
	private DataRecordMetadata[] sourceMetadata;
	private DataRecordMetadata[] targetMetadata;

	private Map<String, String> rules = new HashMap<String, String>();
	private Map<Integer[], Rule> transformMap = new HashMap<Integer[], Rule>();
	
	private Map<String, Sequence> sequences;
	
	private static final int REC_NO = 0;
	private static final int FIELD_NO = 1;
	
	private static final char DOT = '.';
	
	private Entry<Integer[], Rule> fieldRule;
	private int ruleType;
	private Integer[] field = new Integer[2];
	private Rule rule;
	private String sequenceName;


	public void addRule(String patternOut, String pattern) {
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field, pattern);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addRule(int recNo, int fieldNo, String value){
		rules.put(String.valueOf(recNo) + DOT + String.valueOf(fieldNo), value);
	}
	
	public void addRule(int recNo, String field, String value){
		rules.put(String.valueOf(recNo) + DOT + field, value);
	}
	
	public void addRule(int fieldNo, String value){
		addRule(0, fieldNo, value);
	}
	
	public void addRule(String patternOut, int recNo, int fieldNo){
		String field = resolveField(patternOut);
		if (field != null) {
			rules.put(field,  String.valueOf(recNo) + DOT + String.valueOf(fieldNo));
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addRule(String patternOut, int recNo, String field){
		String outField = resolveField(patternOut);
		if (outField != null) {
			rules.put(outField,  String.valueOf(recNo) + DOT + field);
		}else{
//			throw new TransformException("Wrong output field mask");
		}
	}
	
	public void addRule(String patternOut, int fieldNo){
		addRule(patternOut,0,fieldNo);
	}
	
	public void addRule(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo){
		rules.put(String.valueOf(outRecNo) + DOT + String.valueOf(outFieldNo), 
				String.valueOf(inRecNo) + DOT + String.valueOf(inFieldNo));
	}
	
	public void finished() {
		// TODO Auto-generated method stub

	}

	public TransformationGraph getGraph() {
		// TODO Auto-generated method stub
		return null;
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
		this.parameters=parameters;
		this.sourceMetadata=sourcesMetadata;
		this.targetMetadata=targetMetadata;
	    return init();
	}

	private boolean init() throws ComponentNotReadyException{
		Entry<String, String> rulesEntry;
		Rule rule;
		ArrayList<String> outFields;
		ArrayList<String> inFields;
		for (Iterator<Entry<String, String>> i = rules.entrySet().iterator();i.hasNext();){
			rulesEntry = i.next();
		}
		return true;
	}
	
	private ArrayList<String> findFields(String pattern,DataRecordMetadata[] metadata){
		ArrayList<String> list = new ArrayList<String>();
		String recordNoString = pattern.substring(0,pattern.indexOf(DOT));
		Pattern recordPattern = Pattern.compile(recordNoString);
		Matcher recordMatcher;
		Pattern fieldPattern = Pattern.compile(pattern.substring(pattern.indexOf(DOT)+1));
		Matcher fieldMatcher;
		int recNo = -1;
		try {
			recNo = Integer.parseInt(recordNoString);
			for (int i=0;i<metadata[recNo].getNumFields();i++){
				fieldMatcher = fieldPattern.matcher(metadata[recNo].getField(i).getName());
				if (fieldMatcher.matches()){
					list.add(String.valueOf(recNo) + DOT + metadata[recNo].getField(i).getName());
				}
			}
		}catch (NumberFormatException e){
			for (int i=0;i<metadata.length;i++){
				recordMatcher = recordPattern.matcher(metadata[i].getName());
				if (recordMatcher.matches()){
					for (int j=0;j<metadata[i].getNumFields();j++){
						fieldMatcher = fieldPattern.matcher(metadata[i].getField(j).getName());
						if (fieldMatcher.matches()){
							list.add(String.valueOf(i) + DOT + metadata[i].getField(j).getName());
						}
					}
				}
			}
		}
		return list;
	}
	
	public void setGraph(TransformationGraph graph) {
		// TODO Auto-generated method stub

	}

	public void signal(Object signalObject) {
		// TODO Auto-generated method stub

	}

	public boolean transform(DataRecord[] sources, DataRecord[] target)
			throws TransformException {
		for (Iterator<Map.Entry<Integer[], Rule>> i = transformMap.entrySet().iterator();i.hasNext();){
			fieldRule = i.next();
			ruleType = fieldRule.getValue().type;
			field = fieldRule.getKey();
			rule = fieldRule.getValue();
			if (ruleType == Rule.FIELD) {
				target[field[REC_NO]].getField(field[FIELD_NO])
						.setValue(rule.getValue(sources));
			}else if (ruleType == Rule.SEQUENCE){
				sequenceName = rule.value.substring(0, rule.value.indexOf(DOT));
				target[field[REC_NO]].getField(field[FIELD_NO])
					.setValue(rule.getValue(sequences.get(sequenceName)));
			}else{
				target[field[REC_NO]].getField(field[FIELD_NO]).fromString(rule.getValue());
			}
		}
		return true;
	}
	
	private String resolveField(String pattern){
		String[] parts = pattern.split(String.valueOf(DOT));
		switch (parts.length) {
		case 2:
			if (parts[0].startsWith("$")){// ${recNo.field}
				return parts[0].substring(2) + parts[1].substring(0,parts[1].length()-1);
			}else{// recNo.field
				return pattern;
			}
		case 3:
			if (parts[0].startsWith("$")){// ${out.recNo.field}
				return parts[1] + parts[2].substring(0,parts[2].length() -1);
			}else{//out.recNo.field
				return parts[1] + parts[2];
			}
		default:return null;
		}
	}
	
	class Rule {
		
		final static int FIELD = 0;
		final static int CONSTANT = 1;
		final static int SEQUENCE = 2;
		
		int type;
		String value;
		
		Rule(int type, String value){
			this.type = type;
			this.value = value;
		}
		
		String getValue(){
			return value;
		}
		
		Object getValue(DataRecord[] records){
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			int recNo = dotIndex > -1 ? Integer.parseInt(value.substring(0, dotIndex)) : 0;
			int fieldNo = dotIndex > -1 ? Integer.parseInt(value.substring(dotIndex + 1)) : Integer.parseInt(value); 
			return records[recNo].getField(fieldNo).getValue();
		}
		
		Object getValue(Sequence sequence){
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			String method = dotIndex > -1 ? value.substring(dotIndex +1) : value;
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
