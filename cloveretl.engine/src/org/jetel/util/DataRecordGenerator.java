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
package org.jetel.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DateDataField;
import org.jetel.data.Defaults;
import org.jetel.data.parser.AbstractParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.string.StringUtils;

/**
 * <h3>Data records Generator - helper for DataGenerator component</h3>
 * 
 * Generates data records according to pattern. Record fields can be filled by constants, random or sequence values.
 * Created by refactoring DataGenerator component.
 * 
 * @see org.jetel.component.DataGenerator for more info
 * 
 * @author Martin Varecha <martin.varecha@javlinconsulting.cz> (c) JavlinConsulting s.r.o. www.javlinconsulting.cz
 * @created Nov 9, 2007
 */
public class DataRecordGenerator extends AbstractParser {

	static Log logger = LogFactory.getLog(DataRecordGenerator.class);

	private final int MIN = 0;
	private final int MAX = 1;
	private final int MULTIPLIER = 2;
	private final int MOVE = 3;

	private Node component;
	private String pattern;
	private DataRecord patternRecord = null;
	private Parser parser;
	private DataRecordMetadata metadata;
	// private DataRecord record;
	private String[] randomFields = null;
	private String[][] randomRanges;
	private DataGenerator random = new DataGenerator();
	private long randomSeed = Long.MIN_VALUE;
	private String[] sequenceFields = null;
	private String[] sequenceIDs;
	private boolean[] randomField;// indicates if i-th field is to fill by random value
	private Object[][] specialValue;// for each field if it is not set from pattern: 0 - min random, 1 - max random, 2 -
	// multiplier = (max random - min random)/(possible max random - possible min
	// random),3 - move

	private long recordNumber;
	private long counter = 0;

	private boolean initialized = false;

	private DataRecord reusableRecord;

	/**
	 * 
	 * @param component:
	 *            instance of component which uses this recordGenerator; May be null if this generator is used without
	 *            component.
	 * @param metadata
	 * @param pattern:
	 *            pattern for filling new record. It is string containing values for <b>all</b> fields, which will be
	 *            not set by random or sequence values. Field's values in this string have to have format coherent with
	 *            metadata (appropriate length or delimited by appropriate delimiter)
	 * @param randomFields:
	 *            names of fields to be set by random values (optionaly with ranges) separated by semicolon. When there
	 *            are not given random ranges (or one of them) there are used minimum possible values for given data
	 *            field (eg. for LongDataField minimum is Long.MIN_VALUE and maximum Long.MAX_VALUE). Random strings are
	 *            generated from chars 'a' till 'z'. For numeric fields random ranges are: min value (inclusive) and max
	 *            value (exclusive), and for byte or string fields random ranges mean minimum and maximum length of
	 *            field (if it is not fixed), eg. field1=random(0,51) - for numeric field random value from range
	 *            (0,50], for string field - random string of length 0 till 51 chars, field2=random(10) - allowed only
	 *            for string or byte field, means length of field
	 * @param randomSeed:
	 *            Sets the seed of this random number generator using a single long seed.
	 * @param sequenceFields:
	 *            names of fields to be set by values from sequence (optionaly with sequence name:
	 *            fieldName=sequenceName) separated by semicolon.
	 * @throws ComponentNotReadyException
	 * @see DataGenerator for more info
	 */
	public DataRecordGenerator(Node component, DataRecordMetadata metadata, String pattern, String randomFields,
			long randomSeed, String sequenceFields, long recordsNumber) throws ComponentNotReadyException {
		this.recordNumber = recordsNumber;
		this.component = component;
		this.metadata = metadata;
		this.pattern = pattern;
		this.setRandomFields(randomFields);
		this.setRandomSeed(randomSeed);
		this.setSequenceFields(sequenceFields);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		counter = 0;
		specialValue = new Object[metadata.getNumFields()][4];
		// create and initialize output record
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();

		reusableRecord = DataRecordFactory.newRecord(metadata);
		reusableRecord.init();

		// create metadata for pattern record - fields are set from pattern (not random and sequence values)
		DataRecordMetadata cutMetadata = metadata.duplicate();

		randomField = new boolean[metadata.getNumFields()];
		Arrays.fill(randomField, false);
		int randomIndex = -1;
		int sequenceIndex = -1;
		DataField tmpField;
		char fieldType;
		// cut random and sequence fields from pattern record
		// prepare random multiplier and move for random fields (against Random class defaults)
		// prepare sequence ID
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if (randomFields != null) {
				randomIndex = StringUtils.findString(metadata.getField(i).getName(), randomFields);
			}
			if (randomIndex > -1) {// field found among random fields
				cutMetadata.delField(metadata.getField(i).getName());
				randomField[i] = true;
				fieldType = metadata.getField(i).getType();
				// prepare special values for random field
				switch (fieldType) {
				case DataFieldMetadata.BOOLEAN_FIELD:
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
				case DataFieldMetadata.STRING_FIELD:
					// special values mean maximum and minimum length of field
					int len = metadata.getField(i).getSize();
					if (len > 0) {
						specialValue[i][MIN] = len;
						specialValue[i][MAX] = len;
					} else {
						if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])) {
							specialValue[i][MIN] = new Integer(randomRanges[randomIndex][MIN]);
						}
						if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])) {
							specialValue[i][MAX] = new Integer(randomRanges[randomIndex][MAX]);
						}
						if (specialValue[i][MIN] == null) {
							specialValue[i][MIN] = fieldType == DataFieldMetadata.STRING_FIELD ? 32 : 8;
						}
						if (specialValue[i][MAX] == null) {
							specialValue[i][MAX] = specialValue[i][MIN];
						}
					}
					break;
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					// prepare min and max from date, prepare multiplier and move
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])) {
						tmpField = record.getField(i).duplicate();
						((DateDataField) tmpField).fromString(randomRanges[randomIndex][MIN]);
						specialValue[i][MIN] = ((DateDataField) tmpField).getDate().getTime();
					} else {
						specialValue[i][MIN] = Long.MIN_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])) {
						tmpField = record.getField(i).duplicate();
						((DateDataField) tmpField).fromString(randomRanges[randomIndex][MAX]);
						specialValue[i][MAX] = ((DateDataField) tmpField).getDate().getTime();
					} else {
						specialValue[i][MAX] = Long.MAX_VALUE;
					}
					// multiplier = (max - min) / (Long.Max - Long.Min)
					specialValue[i][MULTIPLIER] = (((Long) specialValue[i][MAX]).doubleValue() - ((Long) specialValue[i][MIN])
							.doubleValue())
							/ ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE);
					// move = (min*Long.Max - max*Long.Min)/(Long.Max-Long.Min)
					specialValue[i][MOVE] = (((Long) specialValue[i][MIN]).doubleValue() * Long.MAX_VALUE - ((Long) specialValue[i][MAX])
							.doubleValue()
							* Long.MIN_VALUE)
							/ ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					// prepare min and max from double, multiplier = max - min, move = min, used directly in execute()
					// method
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])) {
						specialValue[i][MIN] = new Double(randomRanges[randomIndex][MIN]);
					} else {
						specialValue[i][MIN] = -Double.MAX_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])) {
						specialValue[i][MAX] = new Double(randomRanges[randomIndex][MAX]);
					} else {
						specialValue[i][MAX] = Double.MAX_VALUE;
					}
					break;
				case DataFieldMetadata.INTEGER_FIELD:
					// prepare min and max from integer, prepare multiplier and move
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])) {
						specialValue[i][MIN] = new Integer(randomRanges[randomIndex][MIN]);
					} else {
						specialValue[i][MIN] = Integer.MIN_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])) {
						specialValue[i][MAX] = new Integer(randomRanges[randomIndex][MAX]);
					} else {
						specialValue[i][MAX] = Integer.MAX_VALUE;
					}
					break;
				case DataFieldMetadata.LONG_FIELD:
					// prepare min and max from integer, prepare multiplier and move
					if (!StringUtils.isBlank(randomRanges[randomIndex][MIN])) {
						specialValue[i][MIN] = new Long(randomRanges[randomIndex][MIN]);
					} else {
						specialValue[i][MIN] = Long.MIN_VALUE;
					}
					if (!StringUtils.isBlank(randomRanges[randomIndex][MAX])) {
						specialValue[i][MAX] = new Long(randomRanges[randomIndex][MAX]);
					} else {
						specialValue[i][MAX] = Long.MAX_VALUE;
					}
					break;
				default:
					throw new ComponentNotReadyException("Unknown data field type " + metadata.getField(i).getName()
							+ " : " + metadata.getField(i).getTypeAsString());
				}
			} else {// field not found among random fields
				if (sequenceFields != null) {
					sequenceIndex = StringUtils.findString(metadata.getField(i).getName(), sequenceFields);
					if (sequenceIndex > -1) {// field found among sequence fields
						cutMetadata.delField(metadata.getField(i).getName());
						if (sequenceIDs[sequenceIndex] == null) {// not given sequence id
							// find any sequence in graph
							if (component != null) {
								specialValue[i][0] = component.getGraph().getSequences().hasNext() ? (String) component
										.getGraph().getSequences().next() : null;
							}
							if (specialValue[i][0] == null) {
								throw new ComponentNotReadyException(component,
										"There are no sequences defined in graph!!!");
							}
						} else {
							specialValue[i][0] = sequenceIDs[sequenceIndex];
						}
					}
				}
			}
		}
		// set random seed
		if (randomSeed > Long.MIN_VALUE) {
			random.setSeed(randomSeed);
		}
		if (cutMetadata.getNumFields() > 0) {
			patternRecord = DataRecordFactory.newRecord(cutMetadata);
			patternRecord.init();
			// prepare approperiate data parser
			switch (metadata.getRecType()) {
			case DataRecordMetadata.DELIMITED_RECORD:
				parser = new DelimitedDataParser(cutMetadata,Defaults.DataParser.DEFAULT_CHARSET_DECODER);
				break;
			case DataRecordMetadata.FIXEDLEN_RECORD:
				parser = new FixLenCharDataParser(cutMetadata,Defaults.DataParser.DEFAULT_CHARSET_DECODER);
				break;
			default:
				parser = TextParserFactory.getParser(cutMetadata);
				break;
			}
			parser.init();
			try {
				parser.setDataSource(new ByteArrayInputStream(pattern
						.getBytes(Defaults.DataParser.DEFAULT_CHARSET_DECODER)));
			} catch (UnsupportedEncodingException e) {
				throw new ComponentNotReadyException("Unexpceted error.", e);
			} catch (IOException e) { //no previous data source - this exception shouldn't be thrown
				throw new ComponentNotReadyException("Unexpected error.", e);
			}
			try {
				patternRecord = parser.getNext();
				if (patternRecord == null) {
					// removed 2007-12-12, empty pattern is allowed
					// throw new ComponentNotReadyException(component, "Can't get record from pattern. Specified
					// pattern:" + StringUtils.quote(pattern));
				}
			} catch (BadDataFormatException e) {
				throw new ComponentNotReadyException(component, "Can't get record from pattern: "
						+ StringUtils.quote(pattern), e);
			} catch (JetelException e) {
				throw new ComponentNotReadyException(component, "Can't get record from pattern: "
						+ StringUtils.quote(pattern));
			}
			try {
				parser.close();
			} catch (IOException e) {
				throw new ComponentNotReadyException("Pattern stream reading error, unexpected error.", e);
			}
		}
		// if (component != null) {
		// Iterator<String> sequences = component.getGraph().getSequences();
		// while (sequences.hasNext()) {
		// component.getGraph().getSequence(sequences.next()).init();
		// }
		// }
		initialized = true;
	}

	/**
	 * Returns reusable instance of DataRecord filled according to preset rules.
	 * 
	 * @return
	 * @throws JetelException
	 */
	public DataRecord next() throws JetelException {
		return getNext();
	}

	/**
	 * Returns reusable instance of DataRecord filled according to preset rules.
	 */
	@Override
	public DataRecord getNext() throws JetelException {
		return getNext(reusableRecord);
	}

	/**
	 * Fills specified instance of DataRecord according to preset rules.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		if (!initialized) {
			throw new JetelException("Generator is not initialized!");
		}
		if (counter >= this.recordNumber) {
			return null;
		}
		counter++;
		boolean[] set = record.copyFieldsByName(patternRecord);
		Object value = null;
		Sequence sequence;
		// set constant fields from pattern
		for (int j = 0; j < set.length; j++) {
			if (!set[j]) {// j-th field have not been set yet
				if (randomField != null && randomField[j]) {// set random value
					switch (record.getField(j).getType()) {
					case DataFieldMetadata.BYTE_FIELD:
					case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
						// create new byte array with random length (between given ranges)
						value = new byte[random.nextInt((Integer) specialValue[j][MAX] - (Integer) specialValue[j][MIN]
								+ 1)
								+ (Integer) specialValue[j][MIN]];
						// fill it by random bytes
						random.nextBytes((byte[]) value);
						break;
					case DataFieldMetadata.DATE_FIELD:
					case DataFieldMetadata.DATETIME_FIELD:
					case DataFieldMetadata.LONG_FIELD:
						value = random.nextLong((Long) specialValue[j][MIN], (Long) specialValue[j][MAX]);
						break;
					case DataFieldMetadata.INTEGER_FIELD:
						value = random.nextInt((Integer) specialValue[j][MIN], (Integer) specialValue[j][MAX]);
						break;
					case DataFieldMetadata.BOOLEAN_FIELD:
						value = Boolean.valueOf(Math.random() > 0.5);
						break;
					case DataFieldMetadata.DECIMAL_FIELD:
					case DataFieldMetadata.NUMERIC_FIELD:
						// get random double from given interval
						value = (Double) specialValue[j][MIN] + random.nextDouble()
								* ((Double) specialValue[j][MAX] - (Double) specialValue[j][MIN]);
						break;
					case DataFieldMetadata.STRING_FIELD:
						// create random string of random length (between given ranges)
						value = random.nextString((Integer) specialValue[j][MIN], (Integer) specialValue[j][MAX]);
						break;
					}
					record.getField(j).setValue(value);
				} else {// not from pattern, not random, so sequence
					if (specialValue == null || component == null) {
						continue;
					}
					sequence = component.getGraph().getSequence((String) specialValue[j][0]);
					if (sequence == null) {
						continue;
					}
					switch (record.getField(j).getType()) {
					case DataFieldMetadata.BYTE_FIELD:
					case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
					case DataFieldMetadata.STRING_FIELD:
						record.getField(j).setValue(sequence.nextValueString());
						break;
					case DataFieldMetadata.DECIMAL_FIELD:
					case DataFieldMetadata.NUMERIC_FIELD:
					case DataFieldMetadata.LONG_FIELD:
						record.getField(j).setValue(sequence.nextValueLong());
						break;
					case DataFieldMetadata.INTEGER_FIELD:
						record.getField(j).setValue(sequence.nextValueInt());
						break;
					default:
						throw new JetelException("Can't set value from sequence to field "
								+ metadata.getField(j).getName() + " type - " + metadata.getFieldTypeAsString(j));
					}
				}
			}
		}
		return record;
	}

	/**
	 * Reads names of random fields with ranges from parameter and sets them to global variables randomFields and
	 * randomRanges. If random ranges are not given sets them to empty strings
	 * 
	 * @param randomFields
	 *            the randomFields to set in form fieldName=random(min,max)
	 */
	private void setRandomFields(String randomFields) {
		if (randomFields == null) {
			return;
		}
		String[] fields = StringUtils.split(randomFields);
		this.randomFields = new String[fields.length];
		this.randomRanges = new String[fields.length][2];// 0 - number, 1 - min, 2 - max
		String[] param;
		int leftParenthesisIndex;
		int commaIndex;
		int rightParantesisIndex;
		for (int i = 0; i < fields.length; i++) {
			// new notation $f:=random(min,max), old notation f=random(min,max)
			param = JoinKeyUtils.getMappingItemsFromMappingString(fields[i]);
			this.randomFields[i] = param[0];
			if (param[1] != null) {
				leftParenthesisIndex = param[1].indexOf('(');
				commaIndex = param[1].indexOf(',');
				rightParantesisIndex = param[1].indexOf(')');
				if (commaIndex == -1) {
					randomRanges[i][MIN] = StringUtils.unquote(param[1].substring(leftParenthesisIndex + 1,
							rightParantesisIndex));
					randomRanges[i][MAX] = "";
				} else {
					randomRanges[i][MIN] = StringUtils
							.unquote(param[1].substring(leftParenthesisIndex + 1, commaIndex));
					randomRanges[i][MAX] = StringUtils
							.unquote(param[1].substring(commaIndex + 1, rightParantesisIndex));
				}
				randomRanges[i][MIN] = randomRanges[i][MIN].trim();
				randomRanges[i][MAX] = randomRanges[i][MAX].trim();
			} else {
				randomRanges[i][MIN] = "";
				randomRanges[i][MAX] = "";
			}
		}
	}

	/**
	 * @param randomSeed
	 *            the randomSeed to set
	 */
	private void setRandomSeed(long randomSeed) {
		this.randomSeed = randomSeed;
	}

	/**
	 * Reads names of sequence fields with sequence IDs from parameter and sets them to global variables sequenceFields
	 * and sequenceIDs.
	 * 
	 * @param sequenceFields
	 *            the sequenceFields to set in form fieldName=sequenceName or fieldName only
	 */
	private void setSequenceFields(String sequenceFields) {
		if (sequenceFields == null) {
			return;
		}
		String[] fields = StringUtils.split(sequenceFields);
		this.sequenceFields = new String[fields.length];
		this.sequenceIDs = new String[fields.length];
		String[] param;
		for (int i = 0; i < fields.length; i++) {
			param = JoinKeyUtils.getMappingItemsFromMappingString(fields[i]);
			this.sequenceFields[i] = param[0];
			if (param[1] != null) {
				sequenceIDs[i] = param[1];
			}
		}
	}

	/**
	 * Creates formatted string containing random fields. Usable for XML storage.
	 * 
	 * @return
	 */
	public String getRandomFieldsString() {
		if (randomFields != null) {
			StringBuilder fields = new StringBuilder();
			for (int i = 0; i < randomFields.length; i++) {
				fields.append(Defaults.CLOVER_FIELD_INDICATOR);
				fields.append(randomFields[i]);
				fields.append(Defaults.ASSIGN_SIGN);
				fields.append("random(");
				fields.append(randomRanges[i][MIN]);
				fields.append(',');
				fields.append(randomRanges[i][MAX]);
				fields.append(");");
			}
			return fields.toString();
		}
		return null;
	}

	/**
	 * Creates formatted string containing sequenced fields. Usable for XMl storage.
	 * 
	 * @return
	 */
	public String getSequenceFieldsString() {
		if (sequenceFields != null) {
			StringBuilder fields = new StringBuilder();
			for (int i = 0; i < sequenceFields.length; i++) {
				fields.append(Defaults.CLOVER_FIELD_INDICATOR);
				fields.append(sequenceFields[i]);
				fields.append(Defaults.ASSIGN_SIGN);
				if (sequenceIDs[i] != null) {
					fields.append(sequenceIDs[i]);
				}
				fields.append(";");
			}// for
			return fields.toString();
		}
		return null;
	}

	@Override
	public void close() {
		counter = recordNumber;
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return null;
	}

	@Override
	public PolicyType getPolicyType() {
		return PolicyType.STRICT;
	}

	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		if (_metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		this.metadata = _metadata;
		init();
	}

	@Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		// nonsence for this implementation
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
	}

	@Override
	protected void releaseDataSource() {
		// nonsense for this implementation
	}

	@Override
	public void setReleaseDataSource(boolean releaseInputSource) {
		// nonsense for this implementation
	}

	@Override
	public int skip(int rec) throws JetelException {
		if (counter + rec > recordNumber) {
			long i = recordNumber - counter;
			counter = recordNumber;
			return (int) i; //should be change in the future after the return value of this skip method will be long typed
		} else {
			counter += rec;
			return rec;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
	public void reset() {
		counter = 0;
		
		// set random seed
		if (randomSeed > Long.MIN_VALUE) {
			random.setSeed(randomSeed);
		}
	}

	@Override
	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void movePosition(Object position) {
		// TODO Auto-generated method stub

	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		reset();
	}

	@Override
	public void free() {
		close();
	}

	@Override
	public boolean nextL3Source() {
		return false;
	}
}
