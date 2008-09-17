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

package org.jetel.connection.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * This class can be used for analyzing sql queries which contain mapping
 * between clover and db fields. Example queries:<br>
 * insert into mytable [(f1,f2,...,fn)] values (val1, $field2, ...,$fieldm )
 * returning $key := dbfield1, $field := dbfield2;<br>
 * delete from mytable where f1 = $field1 and ... fn = $fieldn; <br>
 * update mytable set f1 = $field1,...,fn=$fieldn
 * 
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting
 *         s.r.o. www.javlinconsulting.cz
 * 
 * @since Oct 11, 2007
 * 
 */
public class QueryAnalyzer {

	private final static Pattern CLOVER_DB_MAPPING_PATTERN = Pattern
			.compile("(\\$(\\w+)\\s*" + Defaults.ASSIGN_SIGN + "\\s*\\$?)("
					+ SQLUtil.DB_FIELD_PATTERN + ")");// $cloverField:=[$]dbfield
	private final static Pattern DB_CLOVER_MAPPING_PATTERN = Pattern
			.compile("(" + SQLUtil.DB_FIELD_PATTERN
					+ ")\\s*=\\s*[\\?\\$](\\w*)");// dbField = $cloverField or dbField = ?
	private final static Pattern FIELDS_PATTERN = Pattern.compile("\\$?"
			+ SQLUtil.DB_FIELD_PATTERN + "|\\?");// [$]field or ? 
	private final static Pattern CLOVER_FIELDS_PATTERN = Pattern
			.compile("\\$\\w+");// $cloverField
	private final static Pattern DB_FIELDS_PATTERN = Pattern
			.compile(SQLUtil.DB_FIELD_PATTERN);// dbField
	private final static Pattern FIELDS_LIST_PATTERN = Pattern
			.compile("\\((\\s*\\$?" + SQLUtil.DB_FIELD_PATTERN
					+ "|\\?)[\"\'\\.,_\\s\\$\\p{Alnum}\\?]*\\)");// (dbfield1, dbField2 ,..) or (smth, $field1, ...) - can be quoted

	private final static Pattern FROM_TABLE_PATTERN = Pattern.compile("from\\s+" + SQLUtil.DB_FIELD_PATTERN, Pattern.CASE_INSENSITIVE);
	private final static Pattern INTO_TABLE_PATTERN = Pattern.compile("into\\s+" +SQLUtil.DB_FIELD_PATTERN, Pattern.CASE_INSENSITIVE);
	private final static Pattern UPDATE_TABLE_PATTERN = Pattern.compile("update\\s+" + SQLUtil.DB_FIELD_PATTERN, Pattern.CASE_INSENSITIVE);
	
	private String query;
	private String source;
	private boolean queryInCloverFormat;
	private List<String[]> cloverDbFieldMap = new ArrayList<String[]>();
	private List<String[]> dbCloverFieldMap = new ArrayList<String[]>();
	private QueryType queryType;
	private String table = null;

	public final static String RETURNING_KEY_WORD = "returning";

	public enum QueryType{
		INSERT,
		UPDATE,
		DELETE,
		SELECT
	}

	/**
	 * Empty constructor. Must be called setQuery(String) then
	 */
	public QueryAnalyzer() {
	}

	/**
	 * Constructor
	 * 
	 * @param query
	 *            to analyze with db - clover mapping
	 * 
	 */
	public QueryAnalyzer(String query) {
		source = query;
		this.query = query.trim();
		queryInCloverFormat = CLOVER_FIELDS_PATTERN.matcher(query).find();
		queryType = QueryType.valueOf((new StringTokenizer(this.query.toUpperCase(), " \t\n\r\f*").nextToken()));
	}

	/**
	 * Sets new query for analyze. Old results are forgotten.
	 * 
	 * @param query
	 */
	public void setQuery(String query) {
		source = query;
		this.query = query.trim();
		cloverDbFieldMap.clear();
		dbCloverFieldMap.clear();
		queryInCloverFormat = CLOVER_FIELDS_PATTERN.matcher(query).find();
		queryType = QueryType.valueOf((new StringTokenizer(this.query.toUpperCase())).nextToken());
		table = null;
	}

	/**
	 * Analyze update/delete query
	 * 
	 * @return query in form consumable for PreparedStatement object
	 */
	public String getUpdateDeleteQuery() {
		Matcher mappingMatcher = DB_CLOVER_MAPPING_PATTERN.matcher(query); 
		StringBuffer sb = new StringBuffer();
		while(mappingMatcher.find()){
			if (!StringUtils.isEmpty(mappingMatcher.group(4))) {//found dbField = $field
				dbCloverFieldMap.add(new String[]{mappingMatcher.group(1), mappingMatcher.group(4)});
			}else{//found dbField = ?
				dbCloverFieldMap.add(new String[]{mappingMatcher.group(1), null});
			}
			//replace whole mapping by dbField = ?
			mappingMatcher.appendReplacement(sb, mappingMatcher.group(1) + " = ?");
		}
		mappingMatcher.appendTail(sb);
		//get number of updated rows
		int indexReturning = sb.toString().toLowerCase().indexOf(RETURNING_KEY_WORD);
		if (indexReturning > -1) {
			mappingMatcher = CLOVER_DB_MAPPING_PATTERN.matcher(query);
			while(mappingMatcher.find()){
				cloverDbFieldMap.add(new String[]{mappingMatcher.group(2).startsWith(Defaults.CLOVER_FIELD_INDICATOR) ? 
						mappingMatcher.group(2).substring(Defaults.CLOVER_FIELD_INDICATOR.length()) : 
						mappingMatcher.group(2), mappingMatcher.group(3)});
			}
		}
		return indexReturning == -1 ? sb.toString() :  sb.subSequence(0, indexReturning).toString();
	}

	/**
	 * Analyze select query
	 * 
	 * @return query in form consumable for PreparedStatement object
	 */
	public String getSelectQuery() {
		Matcher mappingMatcher = CLOVER_DB_MAPPING_PATTERN.matcher(query);
		StringBuffer sb = new StringBuffer();
		if (mappingMatcher.find()) {
			do {
				cloverDbFieldMap.add(new String[] { mappingMatcher.group(2),
						mappingMatcher.group(3) });
				mappingMatcher.appendReplacement(sb, mappingMatcher.group(3));
			} while (mappingMatcher.find());
		} else {
			String[] fieldList = query.substring("select".length(),
					query.toLowerCase().indexOf("from") - 1).trim().split(",");
			if (fieldList.length != 1 || !fieldList[0].equals("*")) {// not: select * from ...
				for (int i = 0; i < fieldList.length; i++) {
					cloverDbFieldMap.add(new String[] { null,
							fieldList[i].trim() });
				}
			}
		}
		mappingMatcher.appendTail(sb);
		// find dbField = $field or dbField = ? mapping
		mappingMatcher = DB_CLOVER_MAPPING_PATTERN.matcher(sb.toString());
		sb.setLength(0);
		while (mappingMatcher.find()) {
			if (!StringUtils.isEmpty(mappingMatcher.group(4))) {// found dbField = $field
				dbCloverFieldMap.add(new String[] { mappingMatcher.group(1),
						mappingMatcher.group(4) });
			} else {// found dbField = ?
				dbCloverFieldMap.add(new String[] { mappingMatcher.group(1),
						null });
			}
			// replace whole mapping by dbField = ?
			mappingMatcher.appendReplacement(sb, mappingMatcher.group(1)
					+ " = ?");
		}
		mappingMatcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * Analyze insert query
	 * 
	 * @return query in form consumable for PreparedStatement object
	 * @throws SQLException
	 */
	public String getInsertQuery() throws SQLException {
		List<String> dbFields = new ArrayList<String>();
		// find (smth,smth, ..., smth) in query: insert into mytable [(dbfield1, .., dbfieldN)] values ($f1,...,$fn)
		Matcher fieldsListMatcher = FIELDS_LIST_PATTERN.matcher(query);
		Matcher cloverFieldsMatcher = FIELDS_PATTERN.matcher("");
		if (fieldsListMatcher.find()) {
			String fieldsList = fieldsListMatcher.group();
			if (fieldsListMatcher.find()) {// list found 2x: first - db fields, second - clover fields
				Matcher dbFieldsMatcher = DB_FIELDS_PATTERN.matcher(fieldsList);
				// get db fields
				while (dbFieldsMatcher.find()) {
					dbFields.add(dbFieldsMatcher.group());
				}
				cloverFieldsMatcher = FIELDS_PATTERN.matcher(fieldsListMatcher
						.group());
			} else {// list found once only
				cloverFieldsMatcher = FIELDS_PATTERN.matcher(fieldsList);
			}
		}
		// get clover fields
		int index = 0;
		int size = dbFields.size();
		while (cloverFieldsMatcher.find()) {
			if (size > 0 && index >= size) {
				throw new SQLException("Error in sql query: " + query);
			}
			if (cloverFieldsMatcher.group().startsWith(
					Defaults.CLOVER_FIELD_INDICATOR)) {
				dbCloverFieldMap.add(new String[] {	size > 0 ? dbFields.get(index) : null,
						cloverFieldsMatcher.group(1) });
			} else if (index < size) {
				dbCloverFieldMap.add(new String[] { dbFields.get(index), null });
			}
			index++;
		}
		// get autogenerated columns
		int indexReturning = query.toLowerCase().indexOf(RETURNING_KEY_WORD);
		if (indexReturning > -1) {
			Matcher mappingMatcher = CLOVER_DB_MAPPING_PATTERN.matcher(query);
			while (mappingMatcher.find()) {
				cloverDbFieldMap.add(new String[] { mappingMatcher.group(2).startsWith(Defaults.CLOVER_FIELD_INDICATOR) ? 
						mappingMatcher.group(2).substring(Defaults.CLOVER_FIELD_INDICATOR.length()) :
						mappingMatcher.group(2), mappingMatcher.group(3) });
			}
		}
		return indexReturning == -1 ? CLOVER_FIELDS_PATTERN.matcher(query)
				.replaceAll("?") : CLOVER_FIELDS_PATTERN.matcher(
				query.subSequence(0, indexReturning)).replaceAll("?");
	}

	/**
	 * @return clover - db mapping
	 */
	public List<String[]> getCloverDbFieldMap() {
		return cloverDbFieldMap;
	}

	/**
	 * @return db - clover mapping (can have null as a key eg: Insert into abc
	 *         values($f1, sysdate , $f2, $f3))
	 */
	public List<String[]> getDbCloverFieldMap() {
		return dbCloverFieldMap;
	}

	/**
	 * @return query as has been set by user
	 */
	public String getSource() {
		return source;
	}

	@Override
	public String toString() {
		return query != null ? query : source;
	}

	public boolean isQueryInCloverFormat() {
		return queryInCloverFormat;
	}
	
	public QueryType getQueryType() {
		return queryType;
	}

	/**
	 * @return table name from query
	 */
	public String getTable(){
		if (table != null) return table;
		
		Matcher tableMatcher = null;
		switch (queryType) {
		case SELECT:
		case DELETE:
			tableMatcher = FROM_TABLE_PATTERN.matcher(query);
			break;
		case INSERT:
			tableMatcher = INTO_TABLE_PATTERN.matcher(query);
			break;
		case UPDATE:
			tableMatcher = UPDATE_TABLE_PATTERN.matcher(query);
			break;
		}
		if (tableMatcher == null || !tableMatcher.find()) return null;
		
		return tableMatcher.group(1) != null ? tableMatcher.group(1) : tableMatcher.group(2);
	}
}

