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
import java.lang.reflect.Array;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Description of the Class
 *
 * @author      dpavlis
 * @since       9. prosinec 2003
 * @revision    $Revision$
 */
public class RecordFilterOld {

	private final static int CMP_EQ = 0;// equal
	private final static int CMP_LT = -1;// less than
	private final static int CMP_GT = 1;// grater than
	private final static int CMP_LTEQ = -2;// less than equal
	private final static int CMP_GTEQ = 2;// greater than equal
	private final static int CMP_REGEX = 3;// regular expression compare
	private final static int CMP_N_EQL = 5;// not equal

	private final static int AND = 0;
	private final static int OR = 1;
	private final static int NOT = 2;

	private final static String S_CMP_EQ = "==";
	private final static String S_CMP_LT = "<";
	private final static String S_CMP_GT = ">";
	private final static String S_CMP_LTEQ = "=<";
	private final static String S_CMP_GTEQ = ">=";
	private final static String S_CMP_REGEX = "~";
	private final static String S_CMP_N_EQL = "!=";

	private final static String DEFAULT_FILTER_DATE_FORMAT = "yyyy-MM-dd";

	private DataRecordMetadata metadata;
	private FilterItem[] filterSpecs;
	private String filterExpression;


	/**
	 *Constructor for the RecordFilter object
	 *
	 * @param  filterExpression  Description of the Parameter
	 */
	public RecordFilterOld(String filterExpression) {
		this.filterExpression = filterExpression;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  recordMetadata  Description of the Parameter
	 */
	public void init(DataRecordMetadata recordMetadata) {
		String filterField;
		String filterValueStr;
		this.metadata = recordMetadata;
		String[] filterParts = filterExpression.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		filterSpecs = new FilterItem[filterParts.length];
		Map fieldNames = recordMetadata.getFieldNamesMap();
		int operatorIndex;
		int cmpOperator;
		int operatorStrLen;

		for (int i = 0; i < Array.getLength(filterParts); i++) {
			String filterPart = filterParts[i];

			//Get comparison operator
			if ((operatorIndex = filterPart.indexOf(S_CMP_EQ)) != -1) {
				cmpOperator = CMP_EQ;
				operatorStrLen = S_CMP_EQ.length();
			} else if ((operatorIndex = filterPart.indexOf(S_CMP_REGEX)) != -1) {
				cmpOperator = CMP_REGEX;
				operatorStrLen = S_CMP_REGEX.length();
			} else if ((operatorIndex = filterPart.indexOf(S_CMP_LTEQ)) != -1) {
				cmpOperator = CMP_LTEQ;
				operatorStrLen = S_CMP_LTEQ.length();
			} else if ((operatorIndex = filterPart.indexOf(S_CMP_GTEQ)) != -1) {
				cmpOperator = CMP_GTEQ;
				operatorStrLen = S_CMP_GTEQ.length();
			} else if ((operatorIndex = filterPart.indexOf(S_CMP_N_EQL)) != -1) {
				cmpOperator = CMP_N_EQL;
				operatorStrLen = S_CMP_N_EQL.length();
			} // this must go here as there otherwise <= would be reported as less not less or equal
			else if ((operatorIndex = filterPart.indexOf(S_CMP_GT)) != -1) {
				cmpOperator = CMP_GT;
				operatorStrLen = S_CMP_GT.length();
			} else if ((operatorIndex = filterPart.indexOf(S_CMP_LT)) != -1) {
				cmpOperator = CMP_LT;
				operatorStrLen = S_CMP_LT.length();
			} else {
				throw new RuntimeException("Unknown comparison operator: " + filterPart);
			}

			filterField = filterPart.substring(0, operatorIndex).trim();
			filterValueStr = filterPart.substring(operatorIndex + operatorStrLen, filterPart.length());

			DataFieldMetadata fieldMetadata = recordMetadata.getField(filterField);
			if (fieldMetadata == null) {
				throw new RuntimeException("Unknown field name: " + filterField);
			}

			Integer fieldNum = (Integer) fieldNames.get(filterField);
			if (fieldNum == null) {
				throw new RuntimeException("Unknown field name: " + filterField);
			}

			if (cmpOperator == CMP_REGEX) {
				try {
					filterSpecs[i] = new FilterItem(fieldNum.intValue(), new RegexField(filterValueStr), AND);
				} catch (Exception ex) {
					throw new RuntimeException("Error when populating filter's field " + filterField + " with value " + filterValueStr, ex);
				}
			} else {
				DataField filterFieldValue = DataFieldFactory.createDataField(fieldMetadata.getType(), fieldMetadata, false);
				try {
					filterFieldValue.fromString(filterValueStr);
				} catch (Exception ex) {
					throw new RuntimeException("Error when populating filter's field " + filterField + " with value " + filterValueStr, ex);
				}

				filterSpecs[i] = new FilterItem(fieldNum.intValue(), cmpOperator, filterFieldValue, AND);
			}

		}

	}


	/**
	 *  Gets the filterSpecs attribute of the RecordFilter object
	 *
	 * @return    The filterSpecs value
	 */
	public FilterItem[] getFilterSpecs() {
		return filterSpecs;
	}

	public String getFilterExpression() {
		return(this.filterExpression);
	}
	

	/**
	 *  Description of the Method
	 *
	 * @param  record  Description of the Parameter
	 * @return         Description of the Return Value
	 */
	public boolean accepts(DataRecord record) {
		int cmpResult;
		DataField field2Compare;

		for (int i = 0; i < filterSpecs.length; i++) {
			field2Compare = record.getField(filterSpecs[i].getFieldNo());

			if (field2Compare == null) {
				// this should not happen !!
				throw new RuntimeException("Field (reference) to compare with is NULL !!");
			}

			// special case for REGEX
			if (filterSpecs[i].getComparison() == CMP_REGEX) {
				cmpResult = ((RegexField) filterSpecs[i].getValue()).compareTo(field2Compare);
			} else {
				cmpResult = ((DataField) filterSpecs[i].getValue()).compareTo(field2Compare);
			}

			switch (filterSpecs[i].getComparison()) {

				case CMP_EQ:
					if (cmpResult == 0) {
						return true;
					}
					break;// equal
				case CMP_LT:
					if (cmpResult > 0) {
						return true;
					}
					break;// less than
				case CMP_GT:
					if (cmpResult < 0) {
						return true;
					}
					break;// grater than
				case CMP_LTEQ:
					if (cmpResult >= 0) {
						return true;
					}
					break;// less than equal
				case CMP_GTEQ:
					if (cmpResult <= 0) {
						return true;
					}
					break;// greater than equal
				case CMP_N_EQL:
					if (cmpResult != 0) {
						return true;
					}
					break;
				case CMP_REGEX:
					if (cmpResult == 0) {
						return true;
					}
					break;
				default:
					throw new RuntimeException("Unsupported cmparison operator !");
			}

		}

		return false;
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since
	 * @revision    $Revision$
	 */
	private class FilterItem {

		int comparison;
		Object value;
		int logicalOperator;
		char fieldType;
		int fieldNo;


		/**
		 *Constructor for the FilterItem object
		 *
		 * @param  fieldNo    Description of the Parameter
		 * @param  cmp        Description of the Parameter
		 * @param  value      Description of the Parameter
		 * @param  lOperator  Description of the Parameter
		 */
		FilterItem(int fieldNo, int cmp, DataField value, int lOperator) {
			this.fieldNo = fieldNo;
			this.comparison = cmp;
			this.value = value;
			this.fieldType = value.getType();
			this.logicalOperator = lOperator;
		}


		/**
		 *Constructor for the FilterItem object
		 *
		 * @param  fieldNo    Description of the Parameter
		 * @param  value      Description of the Parameter
		 * @param  lOperator  Description of the Parameter
		 */
		FilterItem(int fieldNo, RegexField value, int lOperator) {
			this.fieldNo = fieldNo;
			this.comparison = CMP_REGEX;
			this.value = value;
			this.fieldType = 'X';// X for regeX
			this.logicalOperator = lOperator;
		}


		/**
		 *  Gets the comparison attribute of the FilterItem object
		 *
		 * @return    The comparison value
		 */
		final int getComparison() {
			return comparison;
		}


		/**
		 *  Gets the value attribute of the FilterItem object
		 *
		 * @return    The value value
		 */
		final Object getValue() {
			return value;
		}


		/**
		 *  Gets the logicalOperator attribute of the FilterItem object
		 *
		 * @return    The logicalOperator value
		 */
		final int getLogicalOperator() {
			return logicalOperator;
		}


		/**
		 *  Gets the fieldNo attribute of the FilterItem object
		 *
		 * @return    The fieldNo value
		 */
		final int getFieldNo() {
			return fieldNo;
		}


		/**
		 *  Description of the Method
		 *
		 * @return    Description of the Return Value
		 */
		@Override
		public String toString() {
			return fieldNo + ":" + comparison + ":" + value;
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since
	 * @revision    $Revision$
	 */
	private class RegexField {

		private Pattern p;


		/**
		 *Constructor for the RegexField object
		 *
		 * @param  patternStr  Description of the Parameter
		 */
		RegexField(String patternStr) {
			p = Pattern.compile(patternStr);
		}


		/**
		 *  Description of the Method
		 *
		 * @return    Description of the Return Value
		 */
		@Override
		public String toString() {
			return p.toString();
		}


		/**
		 *  Description of the Method
		 *
		 * @param  _str  Description of the Parameter
		 */
		public void fromString(String _str) {
			p = Pattern.compile(_str);
		}


		/**
		 *  Description of the Method
		 *
		 * @param  obj  Description of the Parameter
		 * @return      Description of the Return Value
		 */
		@Override
		public boolean equals(Object obj) {
			Matcher m = p.matcher(((StringDataField) obj).getCharSequence());
			return m.matches();
		}


		/**
		 *  Description of the Method
		 *
		 * @param  obj  Description of the Parameter
		 * @return      Description of the Return Value
		 */
		public int compareTo(Object obj) {
			return equals(obj) ? 0 : -1;
		}
	}

}

