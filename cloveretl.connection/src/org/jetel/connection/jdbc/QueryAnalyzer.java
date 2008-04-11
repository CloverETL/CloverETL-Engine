
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.primitive.DuplicateKeyMap;
import org.jetel.util.string.StringUtils;

/**
 * This class can be used for analyzing sql queries which contain mapping between clover and db fields.
 * Example queries:<br>
 * insert into mytable [(f1,f2,...,fn)] values (val1, $field2, ...,$fieldm ) returning $key := dbfield1, $field := dbfield2;<br>
 * delete from mytable where f1 = $field1 and ... fn = $fieldn; <br>
 * update mytable set f1 = $field1,...,fn=$fieldn
 * 
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 11, 2007
 *
 */
public class QueryAnalyzer {
	
	public final static String ASSIGN_SIGN = ":=";
	public final static char CLOVER_FIELD_PREFIX_CHAR = '$';
	public final static String CLOVER_FIELD_PREFIX = String.valueOf(CLOVER_FIELD_PREFIX_CHAR);

	private final static Pattern CLOVER_DB_MAPPING_PATTERN = Pattern.compile("(\\$([\\w\\?]+)\\s*" + ASSIGN_SIGN + "\\s*)([\\w.]+)");//$cloverField:=dbfield
	private final static Pattern DB_CLOVER_MAPPING_PATTERN = Pattern.compile("(([\\w]+)\\s*=)\\s*[\\?\\$](\\w*)");//dbField = $cloverField or dbField = ?
	private final static Pattern FIELDS_PATTERN = Pattern.compile("\\$?([\\w.]+)|\\?");//[$]field or ?
	private final static Pattern CLOVER_FIELDS_PATTERN = Pattern.compile("\\$\\w+");//$cloverField
	private final static Pattern DB_FIELDS_PATTERN = Pattern.compile("\\w+");//dbField
	private final static Pattern FIELDS_LIST_PATTERN = Pattern.compile("\\((\\s*\\$?\\w+|\\?)[\\.,\\s\\$\\w\\?]*\\)");//(dbfield1,dbField2,..) or (smth, $field1, ...)
	
	private String query;
	private String source;
	private HashMap<String, String> cloverDbFieldMap = new LinkedHashMap<String, String>();
	private DuplicateKeyMap dbCloverFieldMap = new DuplicateKeyMap(new LinkedHashMap<String, String>());
	
	public final static String RETURNING_KEY_WORD = "returning";
	
	/**
	 * Empty constructor. Must be called setQuery(String) then
	 */
	public QueryAnalyzer(){
	}
	
	/**
	 * Constructor
	 * 
	 * @param query to analyze with db - clover mapping
	 *  
	 */
	public QueryAnalyzer(String query){
		source = query;
		this.query = query.trim();
	}
	
	/**
	 * Sets new query for analyze. Old results are forgotten.
	 * 
	 * @param query
	 */
	public void setQuery(String query){
		source = query;
		this.query = query.trim();
		cloverDbFieldMap.clear();
		dbCloverFieldMap.clear();
	}
	
	/**
	 * Analyze select/update/delete query
	 * 
	 * @return query in form consumable for PreparedStatement object 
	 */
	public String getNotInsertQuery() {
		//find $field:=dbfield mapping
		Matcher mappingMatcher = CLOVER_DB_MAPPING_PATTERN.matcher(query);
		StringBuffer sb = new StringBuffer();
		while(mappingMatcher.find()){
			cloverDbFieldMap.put(mappingMatcher.group(2), mappingMatcher.group(3));
			mappingMatcher.appendReplacement(sb, mappingMatcher.group(3));
		}
		mappingMatcher.appendTail(sb);
		//find dbField = $field or dbField = ? mapping
		mappingMatcher = DB_CLOVER_MAPPING_PATTERN.matcher(sb.toString()); 
		sb.setLength(0);
		while(mappingMatcher.find()){
			if (!StringUtils.isEmpty(mappingMatcher.group(3))) {//found dbField = $field
				dbCloverFieldMap.put(mappingMatcher.group(2), mappingMatcher.group(3));
			}else{//found dbField = ?
				dbCloverFieldMap.put(mappingMatcher.group(2), null);
			}
			//replace whole mapping by dbField = ?
			mappingMatcher.appendReplacement(sb, mappingMatcher.group(1) + "?");
		}
		mappingMatcher.appendTail(sb);
		return sb.toString();
	}
		
	/**
	 * Analyze insert query
	 * 
	 * @return query in form consumable for PreparedStatement object 
	 */
	public String getInsertQuery() throws SQLException{
		List<String> dbFields = new ArrayList<String>();
		//find (smth,smth, ..., smth) in query: insert into mytable [(dbfield1, .., dbfieldN)] values ($f1,...,$fn)
		Matcher fieldsListMatcher = FIELDS_LIST_PATTERN.matcher(query);
		Matcher cloverFieldsMatcher = null;
		if (fieldsListMatcher.find()) {
			String fieldsList = fieldsListMatcher.group();
			if (fieldsListMatcher.find()) {//list found 2x: first - db fields, second - clover fields
				Matcher dbFieldsMatcher = DB_FIELDS_PATTERN.matcher(fieldsList);
				//get db fields
				while (dbFieldsMatcher.find()) {
					dbFields.add(dbFieldsMatcher.group());
				}
				cloverFieldsMatcher = FIELDS_PATTERN.matcher(fieldsListMatcher.group()); 
			}else{//list found once only
				cloverFieldsMatcher = FIELDS_PATTERN.matcher(fieldsList);
			}
		}
		//get clover fields
		int index = 0;
		int size = dbFields.size();
		while(cloverFieldsMatcher.find()){
			if (size > 0 && index >= size) {
				throw new SQLException("Error in sql query: " + query);
			}
			if (cloverFieldsMatcher.group().startsWith(CLOVER_FIELD_PREFIX)) {
				dbCloverFieldMap.put(size > 0 ? dbFields.get(index) : null, cloverFieldsMatcher.group(1));
			}else if (index < size){
				dbCloverFieldMap.put(dbFields.get(index), null);
			}
			index++;
		}
		//get autogenerated columns
		int indexReturning = query.toLowerCase().indexOf(RETURNING_KEY_WORD);
		if (indexReturning > -1) {
			Matcher mappingMatcher = CLOVER_DB_MAPPING_PATTERN.matcher(query);
			while(mappingMatcher.find()){
				cloverDbFieldMap.put(mappingMatcher.group(2), mappingMatcher.group(3));
			}
		}
		return indexReturning == -1 ? 
				CLOVER_FIELDS_PATTERN.matcher(query).replaceAll("?") : 
				CLOVER_FIELDS_PATTERN.matcher(query.subSequence(0, indexReturning)).replaceAll("?");
	}

	/**
	 * @return clover - db mapping
	 */
	public HashMap<String, String> getCloverDbFieldMap() {
		return cloverDbFieldMap;
	}

	/**
	 * @return db - clover mapping (can have null as a key eg: Insert into abc values($f1, sysdate , $f2, $f3))
	 */
	public DuplicateKeyMap getDbCloverFieldMap() {
		return dbCloverFieldMap;
	}

	/**
	 * @return query as has been set by user
	 */
	public String getSource(){
		return source;
	}
	
	@Override
	public String toString() {
		return query != null ? query : source;
	}
}
