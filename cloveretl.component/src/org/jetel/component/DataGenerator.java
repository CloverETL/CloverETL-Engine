
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

import java.io.ByteArrayInputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 21, 2006
 *
 */
public class DataGenerator extends Node {
	
    static Log logger = LogFactory.getLog(DataGenerator.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_GENERATOR";

	/** XML attribute names */
	private static final String XML_PATTERN_ATTRIBUTE = "pattern";
	private static final String XML_RANDOM_FIELDS_ATTRIBUTE = "randomFields";
	private static final String XML_RANDOM_SEED_ATTRIBUTE = "randomSeed";
	private static final String XML_SEQUENCE_FIELDS_ATTRIBUTE = "sequenceFields";
	private static final String XML_RECORDS_NUMBER_ATTRIBUTE = "recordsNumber"; 
	
	private final int MIN = 1;
	private final int MAX = 2;
	private final int MULTIPLIER = 3; 

	private String pattern;
	private DataRecord patternRecord;
	private Parser parser;
	private DataRecordMetadata metadata;
	private DataRecord record;
	private String[] randomFields = null;
	private String[][] randomRanges;
	private Random random;
	private long randomSeed = Long.MIN_VALUE;
	private String[] sequenceFields = null;
	private boolean[] randomField;//indicates if i-th field is to fill by random value
	private Object[][] specialValue;//for each field if it is not set from pattern: 0 - type of value, 1 - min random, 2 - max random, 3 - multiplier = (max random - min random)/(possible max random - possible min random) 
	private int recordsNumber;
	
	public DataGenerator(String id, String pattern, int recordsNumber) {
		super(id);
		this.pattern = pattern;
		this.recordsNumber = recordsNumber;
	}

	public void init() throws ComponentNotReadyException {
        super.init();
        metadata = getOutputPort(0).getMetadata();
        specialValue = new Object[metadata.getNumFields()][4];
		record = new DataRecord(metadata);
		record.init();
        switch (metadata.getRecType()) {
		case DataRecordMetadata.DELIMITED_RECORD:
			parser = new DelimitedDataParser();
			break;
		case DataRecordMetadata.FIXEDLEN_RECORD:
			parser = new FixLenCharDataParser();
			break;
		default:
			parser = new DataParser();
			break;
		}
        DataRecordMetadata cutMetadata = metadata.duplicate();
        randomField = new boolean[metadata.getNumFields()];
		Arrays.fill(randomField, false);
		int randomIndex;
        int sequenceIndex;
        DataField tmpField;
        for (int i=0;i<metadata.getNumFields();i++){
        	randomIndex = StringUtils.findString(metadata.getField(i).getName(), 
        			randomFields);
        	if (randomIndex > -1){
        		cutMetadata.delField(metadata.getField(i).getName());
        		randomField[i] = true;
        		switch (metadata.getField(i).getType()) {
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
					int len = metadata.getField(i).getSize();
					specialValue[i][0] = new byte[len > 0 ? len : 8];
					break;
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					if (randomRanges[i][MIN - 1] != null){
						tmpField = record.getField(i).duplicate();
						((DateDataField)tmpField).fromString(randomRanges[i][MIN - 1]);
						specialValue[i][MIN] = ((DateDataField)tmpField).getDate().getTime();
					}else{
						specialValue[i][MIN] = Long.MIN_VALUE;
					}
					if (randomRanges[i][MAX - 1] != null){
						tmpField = record.getField(i).duplicate();
						((DateDataField)tmpField).fromString(randomRanges[i][MAX - 1]);
						specialValue[i][MAX] = ((DateDataField)tmpField).getDate().getTime();
					}else{
						specialValue[i][MAX] = Long.MAX_VALUE;
					}
					specialValue[i][MULTIPLIER] = 
						((Long)specialValue[i][MAX] - (Long)specialValue[i][MIN])
						/(Long.MAX_VALUE - Long.MIN_VALUE);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					if (randomRanges[i][MIN - 1] != null){
						specialValue[i][MIN] = new Double(randomRanges[i][MIN - 1]);
					}else{
						specialValue[i][MIN] = -Double.MAX_VALUE;
					}
					if (randomRanges[i][MAX - 1] != null){
						specialValue[i][MAX] = new Double(randomRanges[i][MAX - 1]);
					}else{
						specialValue[i][MAX] = Double.MAX_VALUE;
					}
					break;
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
					if (randomRanges[i][MIN - 1] != null){
						specialValue[i][MIN] = new Long(randomRanges[i][MIN - 1]);
					}else{
						specialValue[i][MIN] = 
							metadata.getField(i).getType() == DataFieldMetadata.INTEGER_FIELD ? 
									Integer.MIN_VALUE : Long.MIN_VALUE; 
					}
					if (randomRanges[i][MAX - 1] != null){
						specialValue[i][MAX] = new Long(randomRanges[i][MAX - 1]);
					}else{
						specialValue[i][MAX] = 
							metadata.getField(i).getType() == DataFieldMetadata.INTEGER_FIELD ? 
									Integer.MAX_VALUE : Long.MAX_VALUE; 
					}
					if (metadata.getField(i).getType() == DataFieldMetadata.LONG_FIELD) {
						specialValue[i][MULTIPLIER] = ((Long) specialValue[i][MAX] - (Long) specialValue[i][MIN])
								/ (Long.MAX_VALUE - Long.MIN_VALUE);
					}else{
						specialValue[i][MULTIPLIER] = ((Integer) specialValue[i][MAX] - (Integer) specialValue[i][MIN])
						/ (Integer.MAX_VALUE - Integer.MIN_VALUE);
			}
						break;
				case DataFieldMetadata.STRING_FIELD:
					len = metadata.getField(i).getSize();
					specialValue[i][0] = new StringBuilder(len > 0 ? len : 32);
					break;
				default:
					throw new ComponentNotReadyException(this,"Unknown data field type " + 
							metadata.getField(i).getName() + " : " + metadata.getField(i).getTypeAsString());
				}
        	}else{
        		sequenceIndex = StringUtils.findString(metadata.getField(i).getName(), 
        				sequenceFields);
        		if (sequenceIndex > -1){
            		cutMetadata.delField(metadata.getField(i).getName());
         		}
        	}
        }
		if (randomSeed > Long.MIN_VALUE) {
			random = new Random(randomSeed);
		}else{
			random = new Random();
		}
		parser.init(cutMetadata);
		parser.setDataSource(new ByteArrayInputStream(pattern.getBytes()));
		try {
			patternRecord = parser.getNext();
		} catch (JetelException e) {
			throw new ComponentNotReadyException(this,e);
		}
	}
	

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		// TODO Auto-generated method stub 
		//TODO check metadata on all ports
		return super.checkConfig(status);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		boolean[] set;
		Object value = null;
		Sequence sequence = getGraph().getSequences().hasNext() ? getGraph().getSequence((String)getGraph().getSequences().next()) : null;
		for (int i=0;i<recordsNumber && runIt;i++){
			set = record.copyFieldsByNames(patternRecord);
			for (int j = 0; j < set.length; j++) {
				if (!set[j]){//j-th field have not been set yet 
					if (randomField[j]) {
						switch (record.getField(j).getType()) {
						case DataFieldMetadata.BYTE_FIELD:
						case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
							random.nextBytes((byte[])value);
							break;
						case DataFieldMetadata.DATE_FIELD:
						case DataFieldMetadata.DATETIME_FIELD:
						case DataFieldMetadata.LONG_FIELD:
							value = random.nextLong() * (Double)specialValue[j][MULTIPLIER];
							break;
						case DataFieldMetadata.DECIMAL_FIELD:
						case DataFieldMetadata.NUMERIC_FIELD:
							value = (Double)specialValue[j][MIN] + random.nextDouble()*
							((Double)specialValue[j][MAX] - (Double)specialValue[j][MIN]);
							break;
						case DataFieldMetadata.INTEGER_FIELD:
							value = random.nextInt() * (Double)specialValue[j][MULTIPLIER];
							break;
						case DataFieldMetadata.STRING_FIELD:
							//TODO
							break;
						}
						record.getField(j).setValue(value);
					}else {//not from pattern, not random, so sequence
						record.getField(j).setValue(sequence.nextValueInt());
					}
				}
			}
			writeRecordBroadcast(record);
		}
		broadcastEOF();
		return runIt ? Node.Result.OK : Node.Result.ABORTED;
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		DataGenerator dataGenerator = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			dataGenerator = new DataGenerator(xattribs.getString(XML_ID_ATTRIBUTE), 
					xattribs.getString(XML_PATTERN_ATTRIBUTE), xattribs.getInteger(XML_RECORDS_NUMBER_ATTRIBUTE));
			if (xattribs.exists(XML_RANDOM_FIELDS_ATTRIBUTE)){
				dataGenerator.setRandomFields(xattribs.getString(XML_RANDOM_FIELDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_RANDOM_SEED_ATTRIBUTE)){
				dataGenerator.setRandomSeed(xattribs.getLong(XML_RANDOM_SEED_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SEQUENCE_FIELDS_ATTRIBUTE)){
				dataGenerator.setSequenceFields(xattribs.getString(XML_SEQUENCE_FIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return dataGenerator;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param randomFields the randomFields to set
	 */
	private void setRandomFields(String randomFields) {
		String[] fields = randomFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		this.randomFields = new String[fields.length];
		this.randomRanges = new String[fields.length][2];
		String[] param;
		int parenthesisIndex;
		for (int i = 0; i < fields.length; i++) {
			param = fields[i].split("=");
			this.randomFields[i] = param[0];
			if (param.length > 1){
				parenthesisIndex = param[1].indexOf('('); 
				randomRanges[i][0] = param[1].substring(parenthesisIndex +1,
						param[1].indexOf(','));
				if (parenthesisIndex == -1) {
					randomRanges[i][1] = param[1].substring(param[1].indexOf(',') + 1);
				}else{
					randomRanges[i][1] = param[1].substring(param[1].indexOf(',')+1,
							param[1].indexOf(')'));
				}
			}
		}
	}

	/**
	 * @param randomSeed the randomSeed to set
	 */
	private void setRandomSeed(long randomSeed) {
		this.randomSeed = randomSeed;
	}

	/**
	 * @param sequenceFields the sequenceFields to set
	 */
	private void setSequenceFields(String[] sequenceFields) {
		this.sequenceFields = sequenceFields;
	}

}
