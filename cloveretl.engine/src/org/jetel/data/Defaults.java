/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.data;

/**
 *  Helper class which contains some framework-wide constants defitions.<br>
 *  Change the compile-time defaults here !
 *
 *@author     dpavlis
 *@created    January 23, 2003
 */
public final class Defaults {

	/**
	 *  when buffering IO, what is the default size of the buffer
	 */
	public final static int DEFAULT_INTERNAL_IO_BUFFER_SIZE = 32768;
	
	/**
	 * when creating/parsing date from string, what is the expected/default
	 * format of date
	 */
	
	public final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	public final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public final static String DEFAULT_LOCALE_STR_DELIMITER_REGEX = "\\.";

	/**
	 *  Defaults regarding DataRecord structure/manipulation
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class Record {

		/**
		 *  Determines max size of record (serialized) in bytes.<br>
		 *  If you are getting BufferOverflow, increase the limit here. This
		 *  affects the memory footprint !!!
		 */
		public final static int MAX_RECORD_SIZE = 8192;
	}


	/**
	 *  Defaults for all DataParsers
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataParser {

		/**
		 *  max length of field's value representation (bytes or characters).<br>
		 *  If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public final static int FIELD_BUFFER_LENGTH = 512;

		/**
		 *  default character-decoder to be used if not specified
		 */
		public final static String DEFAULT_CHARSET_DECODER = "ISO-8859-1";
	}


	/**
	 *  Defaults for DataFormatters
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataFormatter {

		/**
		 *  default character-encoder to be used
		 */
		public final static String DEFAULT_CHARSET_ENCODER = "ISO-8859-1";
		/**
		 *  max length of field's value representation (bytes or characters).<br>
		 *  If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public final static int FIELD_BUFFER_LENGTH = 512;

	}


	/**
	 *  Defaults for various components
	 *
	 *@author     david
	 *@created    January 26, 2003
	 */
	public final static class Component {
		/**
		 *  This regular expression is used by various components when parsing
		 *  parameters out of XML attributes.<br>
		 *  When attribute can contain multiple values delimited, this regex
		 *  specifies which are the valid delimiters.
		 */
		 public final static String KEY_FIELDS_DELIMITER_REGEX = "\\s*[:;|]\\s*";
		 
		 /**
		  *  Delimiter character used when exporting components to XML
		  */
		 public final static String KEY_FIELDS_DELIMITER = ";";
	}


	/**
	 *  Defaults for section Data
	 *
	 *@author     dpavlis
	 *@created    6. duben 2003
	 */
	public final static class Data {
		/**
		 *  Unit size of data buffer which keeps data records for sorting/hashing
		 */
		public final static int DATA_RECORDS_BUFFER_SIZE = 10 * 1048576; // 10MB
		/**
		 *  How many units (buffers) can be allocated 
		 */
		public final static short MAX_BUFFERS_ALLOCATED = 99;
		// all together up to 990 MB
	}
	
	/**
	 * Defaults for lookup tables
	 * 
	 * @author david
	 * @since  25.3.2005
	 *
	 */
	public final static class Lookup {
	    
	    /**
	     * Initial size of lookup table (SimpleLookup)
	     */
	    public final static int LOOKUP_INITIAL_CAPACITY = 512;
	    
	}
	
	public final static class WatchDog{
		/**
		 *  how long watchdog thread sleeps (milliseconds) between each awakening.
		 *
		 * @since    July 30, 2002
		 */
		
		public final static int WATCHDOG_SLEEP_INTERVAL = 500;  	
		/**
		 *  how often is watchdog reporting about graph progress
		 *
		 * @since    July 30, 2002
		 */
		public final static int DEFAULT_WATCHDOG_TRACKING_INTERVAL = 30000;

		/**
		 *  One tick is one awakening of watch dog. Sleep interval * number_of_ticks
		 *  determines how often is checked status of each component.<br>
		 *  If watchdog determines that there was an error in some component, the whole
		 *  graph processing is aborted.
		 *
		 * @since    October 1, 2002
		 */
		public final static int NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS = 5;

	}
	
	public final static class GraphProperties{
		
		public final static String PROPERTY_PLACEHOLDER_REGEX = "\\$\\{(\\w+)\\}";
	}
}

