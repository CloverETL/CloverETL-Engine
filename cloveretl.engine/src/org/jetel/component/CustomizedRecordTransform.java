
package org.jetel.component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.WcardPattern;

public class CustomizedRecordTransform implements RecordTransform {
	
	private Properties parameters;
	private DataRecordMetadata[] sourceMetadata;
	private DataRecordMetadata[] targetMetadata;

	private Map<String, String> rules = new LinkedHashMap<String, String>();
	private Map<Integer[], Rule> transformMap = new LinkedHashMap<Integer[], Rule>();
	
	private int rulesIndex = 0;
	private int transformMapIndex = 0;
	
	private static final int REC_NO = 0;
	private static final int FIELD_NO = 1;
	
	private static final char DOT = '.';
	
	private Entry<Integer[], Rule> fieldRule;
	private int ruleType;
	private Integer[] field = new Integer[2];
	private Rule rule;
	private String sequenceID;


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
		if (sourcesMetadata == null || targetMetadata == null)
			return false;
		this.parameters=parameters;
		this.sourceMetadata=sourcesMetadata;
		this.targetMetadata=targetMetadata;
	    return init();
	}

	private boolean init() throws ComponentNotReadyException{
		Entry<String, String> rulesEntry;
		Rule rule;
		int type;
		int recNo;
		int fieldNo;
		String field;
		String ruleString;
		ArrayList<String> outFields;
		ArrayList<String> inFields = null;
		for (Iterator<Entry<String, String>> i = rules.entrySet().iterator();i.hasNext();){
			rulesEntry = i.next();
			outFields = findFields(rulesEntry.getKey(), targetMetadata);
			ruleString = rulesEntry.getValue();
			type = guessRuleType(ruleString);
			Iterator<String> inFieldIterator = null;
			if (type == Rule.FIELD) {
				inFields = findFields(ruleString, sourceMetadata);
				inFieldIterator = inFields.iterator();
			}
			for (Iterator<String> outFieldIterator = outFields.iterator();outFieldIterator.hasNext();){
				field = outFieldIterator.next();
				recNo = Integer.valueOf(field.substring(0, field.indexOf(DOT)));
				fieldNo = Integer.valueOf(field.substring(field.indexOf(DOT)+1));
				rule = transformMap.remove(new Integer[]{recNo,fieldNo});
				if (rule == null) {
					rule = new Rule(type,null);
				}else{
					rule.setType(type);
				}
				if (type != Rule.FIELD){
					rule.setValue(ruleString);
				}else{
					rule.setValue(inFieldIterator.next());
				}
				transformMap.put(new Integer[]{recNo,fieldNo}, rule);
			}
		}
		return true;
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
			switch (ruleType) {
			case Rule.FIELD:
				target[field[REC_NO]].getField(field[FIELD_NO]).setValue(
						rule.getValue(sources));
				break;
			case Rule.SEQUENCE:
				sequenceID = rule.value.substring(0, rule.value.indexOf(DOT));
				target[field[REC_NO]].getField(field[FIELD_NO]).setValue(
						rule.getValue(getGraph().getSequence(sequenceID)));
				break;
			case Rule.PARAMETER:
				target[field[REC_NO]].getField(field[FIELD_NO]).fromString(
						getGraph().getGraphProperties().getProperty(rule.getValue()));
				break;
			default://constant
				target[field[REC_NO]].getField(field[FIELD_NO]).fromString(rule.getValue());
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

	int guessRuleType(String pattern){
		String field = CustomizedRecordTransform.resolveField(pattern);
		if (field != null) {
			if (field.toLowerCase().startsWith("seq.") || 
					field.substring(field.indexOf(DOT)+1).startsWith("next") || 
					field.substring(field.indexOf(DOT)+1).startsWith("current")) {
				return Rule.SEQUENCE;
			}else{
				return Rule.FIELD;
			}
		}else{
			if (pattern.startsWith("$")){
				return Rule.PARAMETER;
			}else{
				return Rule.CONSTANT;
			}
		}
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

	public Map<String, String> getRules() {
		return rules;
	}

	public ArrayList<String> getResolvedRules() {
		ArrayList<String> list = new ArrayList<String>(transformMap.size());
		Entry<Integer[], Rule> entry;
		for (Iterator<Entry<Integer[], Rule>> i = transformMap.entrySet().iterator();i.hasNext();){
			entry = i.next();
			list.add(String.valueOf(entry.getKey()[REC_NO]) + DOT + entry.getKey()[FIELD_NO] + 
					"=" + entry.getValue().getValue());
		}
		return list;
	}

}
