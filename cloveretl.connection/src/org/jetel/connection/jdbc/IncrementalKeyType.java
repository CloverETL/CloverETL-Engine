package org.jetel.connection.jdbc;

/**
 * Types of database incremental key
 * 
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jul 24, 2008
 */
public enum IncrementalKeyType {
	FIRST,//first record from result set
	LAST,//last record from result set
	MAX,//maximal value from result set
	MIN;//minimal value from result set
	
	/**
	 * @return pattern for parsing key definition (all values delimited by | )
	 */
	public static String getKeyTypePattern(){
		StringBuilder pattern = new StringBuilder();
		for (IncrementalKeyType type : IncrementalKeyType.values()) {
			pattern.append(type);
			pattern.append('|');
		}
		return pattern.substring(0, pattern.length() -1);
	}
}
