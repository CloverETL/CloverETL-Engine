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
package org.jetel.component.partition;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.jetel.component.TransformFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.Node;
import org.jetel.lookup.RangeLookupTable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;

/**
 * This factory provides PartitionFunction implementation based on various settings.
 * The factory is used directly from Partition component and PartitionWriter component.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 1.10.2009
 */
public class PartitionFunctionFactory {
	public static final char EQUAL = '=';
	public static final char COMMA = ',';
	public static final char START_OPENED = '(';
	public static final char START_CLOSED = '<';
	public static final char END_OPENED = ')';
	public static final char END_CLOSED = '>';

	private static final String PORT_NO_FIELD_NAME = "portNo"; 

	public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+" + PartitionTL.GET_OUTPUT_PORT_FUNCTION_NAME);

	private Node node;

	private DataRecordMetadata metadata;
	
	private String charset;
	
	private String[] partitionRanges;
	
	private String[] partitionKeyNames;
	
	private boolean useI18N;
	
	private String locale;

	//runtime variables
	private boolean[] startInclude;
	private boolean[] endInclude;

	public PartitionFunction createPartitionFunction(String partitionSource, String partitionClass, String partitionURL) throws ComponentNotReadyException {
		PartitionFunction partitionFunction;
		
		//directly defined source code of partition function has highest priority
    	TransformFactory<PartitionFunction> transformFactory = TransformFactory.createTransformFactory(PartitionFunctionDescriptor.newInstance());
    	transformFactory.setTransform(partitionSource);
    	transformFactory.setTransformClass(partitionClass);
    	transformFactory.setTransformUrl(partitionURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(node);
    	transformFactory.setInMetadata(metadata);
    	if (transformFactory.isTransformSpecified()) {
    		partitionFunction = transformFactory.createTransform();
    	} else {
    		//if no source code is defined, let's analyze other attributes
    		if (partitionRanges != null) {
				//create RangePartition function from partitionKey and partitionRanges
				String rangesData = analyzeRangesString(partitionRanges);
				//create metadata for RangeLookupTable
				DataRecordMetadata lookupMetadata = new DataRecordMetadata("lookup_metadata", DataRecordParsingType.DELIMITED);
				lookupMetadata.addField(new DataFieldMetadata(PORT_NO_FIELD_NAME, DataFieldType.INTEGER, String.valueOf(EQUAL)));
				DataFieldMetadata keyField, startField, endField;
				String[] startFields = new String[partitionKeyNames.length];
				String[] endFields = new String[partitionKeyNames.length];
				//create startField and endField for each key field
				for (int i = 0; i < partitionKeyNames.length; i++) {
					keyField = metadata.getField(partitionKeyNames[i]);
					startField = keyField.duplicate();
					startField.setAutoFilling(null);
					startField.setName(keyField.getName() + "_start");
					startField.setDelimiter(String.valueOf(COMMA));
					startFields[i] = startField.getName(); 
					endField = keyField.duplicate();
					endField.setAutoFilling(null);
					endField.setName(keyField.getName() + "_end");
					endField.setDelimiter(i == partitionKeyNames.length -1 ? 
							Defaults.Component.KEY_FIELDS_DELIMITER : String.valueOf(COMMA));
					endFields[i] = endField.getName();
					lookupMetadata.addField(startField);
					lookupMetadata.addField(endField);
				}
				//create RangeLookupTable 
				RangeLookupTable table = new RangeLookupTable(node.getId() + "_lookupTable", 
						lookupMetadata, startFields, endFields, new DelimitedDataParser(lookupMetadata));
				table.setUseI18N(useI18N);
				table.setLocale(locale);
				table.setData(rangesData);
				table.setStartInclude(startInclude);
				table.setEndInclude(endInclude);
				int rejectedPort = node.getOutPorts().size();
				partitionFunction = new RangePartition(table,
						lookupMetadata.getFieldPosition(PORT_NO_FIELD_NAME), 
						partitionRanges.length < rejectedPort ? rejectedPort -1 : RangePartition.NONEXISTENT_REJECTED_PORT);
			} else if (partitionKeyNames != null) {
				partitionFunction = new HashPartition();
			} else {
				partitionFunction = new RoundRobinPartition();
			}
	
			partitionFunction.setNode(node);
    	}
			
		return partitionFunction;
	}
	
	/**
	 * @param ranges ranges defined by user
	 * @return (0=start00,end00,start01,end01,...,start0n,end0n;1=start10,end10,start11,end11,...;m=startmn,endmn;)
	 */
	private String analyzeRangesString(String[] ranges) throws ComponentNotReadyException {
		StringBuilder rangesData = new StringBuilder();
		int intervalNumbers = ranges[0].split("[(<]").length - 1;
		if (intervalNumbers == 0) {//old reprezentation: x1;x2;..;xn
			//corresponding new form: (,x1>;(x1,x2>;(x2,.....,xn>;(xn,>
			String[] newRanges = new String[ranges.length + 1];
			String start = "";
			String end = ranges[0];
			for (int i = 0; i < newRanges.length; i++) {
				newRanges[i] = START_OPENED + start + COMMA + end + END_CLOSED;
				start = end;
				end = i+1 < newRanges.length - 1 ? ranges[i+1] : "";
			}
			intervalNumbers = 1;
			ranges = newRanges;
		}
		//prepare default start/endInclude
		startInclude = new boolean[intervalNumbers];
		endInclude = new boolean[intervalNumbers];
		Arrays.fill(startInclude, true);
		Arrays.fill(endInclude, false);
		
		int intervalNumber;
		char c;
		//remove brackets, set requested start/endInclude
		for (int i=0; i < ranges.length; i++){
			intervalNumber = 0;
			ranges[i] = ranges[i].trim();
			rangesData.append(i);
			rangesData.append(EQUAL);
			for (int index = 0; index < ranges[i].length(); index++){
				c = ranges[i].charAt(index);
				switch (c) {
				case START_OPENED:
					if (i == 0) {//first '(' - set startInclude to "false"
						startInclude[intervalNumber] = false;
					}else if (startInclude[intervalNumber]) {//there was "<" for this interval before
						node.getLog().warn("startInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					if (intervalNumber != 0){
						rangesData.append(COMMA);
					}
					break;
				case START_CLOSED:
					if (!startInclude[intervalNumber]) {//there was "(" for this interval before
						node.getLog().warn("startInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					if (intervalNumber != 0){
						rangesData.append(COMMA);
					}
					break;
				case END_OPENED:
					if (endInclude[intervalNumber]) {//there was ">" for this interval before
						node.getLog().warn("endInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					intervalNumber++;
					break;
				case END_CLOSED:
					if (i == 0) {//first '>' - set endInclude to "true"
						endInclude[intervalNumber] = true;
					}else if (!endInclude[intervalNumber]) {//there was ")" for this interval before
						node.getLog().warn("endInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					intervalNumber++;
					break;
				default:
					rangesData.append(c);
					break;
				}
			}
			rangesData.append(Defaults.Component.KEY_FIELDS_DELIMITER);
		}
		
		return rangesData.toString();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setPartitionRanges(String[] partitionRanges) {
		this.partitionRanges = partitionRanges;
	}

	public void setPartitionKeyNames(String[] partitionKeyNames) {
		this.partitionKeyNames = partitionKeyNames;
	}

	public void setUseI18N(boolean useI18N) {
		this.useI18N = useI18N;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	/**
	 * @param partitionSource
	 * @param partitionClass
	 * @param partitionURL
	 */
	public void checkConfig(ConfigurationStatus status, String partitionSource, String partitionClass, String partitionURL) {
    	TransformFactory<PartitionFunction> transformFactory = TransformFactory.createTransformFactory(PartitionFunctionDescriptor.newInstance());
    	transformFactory.setTransform(partitionSource);
    	transformFactory.setTransformClass(partitionClass);
    	transformFactory.setTransformUrl(partitionURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(node);
    	transformFactory.setInMetadata(metadata);
    	if (transformFactory.isTransformSpecified()) {
    		transformFactory.checkConfig(status);
    	}
	}

}
