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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *  Helper class which contains some framework-wide constants defitions.<br>
 *  Change the compile-time defaults here !
 *
 *@author     dpavlis
 *@created    January 23, 2003
 */
public final class Defaults {
	private static Properties properties;

	private static void initProperties() {
		InputStream in;
		properties = new Properties();
		try {
			in = Defaults.class.getResourceAsStream("defaultProperties");
			properties.load(in);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static int getIntProperties(String key, int def) {
		return new Integer(properties.getProperty(key, Integer.toString(def))).intValue();
	}

	private static short getShortProperties(String key, short def) {
		return new Short(properties.getProperty(key, Short.toString(def))).shortValue();
	}
	
	private static String getStringProperties(String key, String def) {
		return properties.getProperty(key, def);
	}

	public static void init() {
		initProperties();
		
		DEFAULT_INTERNAL_IO_BUFFER_SIZE = getIntProperties("DEFAULT_INTERNAL_IO_BUFFER_SIZE", 32768);
		DEFAULT_DATE_FORMAT = getStringProperties("DEFAULT_DATE_FORMAT", "yyyy-MM-dd");
		DEFAULT_DATETIME_FORMAT = getStringProperties("DEFAULT_DATETIME_FORMAT", "yyyy-MM-dd HH:mm:ss");
		DEFAULT_LOCALE_STR_DELIMITER_REGEX = getStringProperties("DEFAULT_LOCALE_STR_DELIMITER_REGEX", "\\.");
		
		Record.init();
		DataParser.init();
		DataFormatter.init();
		Component.init();
		Data.init();
		Lookup.init();
		WatchDog.init();
		GraphProperties.init();
	}
	
	/**
	 *  when buffering IO, what is the default size of the buffer
	 */
	public static int DEFAULT_INTERNAL_IO_BUFFER_SIZE;// = 32768;

	/**
	 * when creating/parsing date from string, what is the expected/default
	 * format of date
	 */
	
	public static String DEFAULT_DATE_FORMAT;// = "yyyy-MM-dd";
	public static String DEFAULT_DATETIME_FORMAT;// = "yyyy-MM-dd HH:mm:ss";

	public static String DEFAULT_LOCALE_STR_DELIMITER_REGEX;// = "\\.";

	/**
	 *  Defaults regarding DataRecord structure/manipulation
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class Record {
		public static void init() {
			MAX_RECORD_SIZE = getIntProperties("Record.MAX_RECORD_SIZE", 8192);
		}
		
		/**
		 *  Determines max size of record (serialized) in bytes.<br>
		 *  If you are getting BufferOverflow, increase the limit here. This
		 *  affects the memory footprint !!!
		 */
		public static int MAX_RECORD_SIZE;// = 8192;
	}


	/**
	 *  Defaults for all DataParsers
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataParser {
		public static void init() {
			FIELD_BUFFER_LENGTH = getIntProperties("DataParser.FIELD_BUFFER_LENGTH", 512);
			DEFAULT_CHARSET_DECODER = getStringProperties("DataParser.DEFAULT_CHARSET_DECODER", "ISO-8859-1");
		}

		/**
		 *  max length of field's value representation (bytes or characters).<br>
		 *  If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public static int FIELD_BUFFER_LENGTH;// = 512;

		/**
		 *  default character-decoder to be used if not specified
		 */
		public static String DEFAULT_CHARSET_DECODER;// = "ISO-8859-1";
	}


	/**
	 *  Defaults for DataFormatters
	 *
	 *@author     dpavlis
	 *@created    January 23, 2003
	 */
	public final static class DataFormatter {
		public static void init() {
			DEFAULT_CHARSET_ENCODER = getStringProperties("DataFormatter.DEFAULT_CHARSET_ENCODER", "ISO-8859-1");
			FIELD_BUFFER_LENGTH = getIntProperties("DataFormatter.FIELD_BUFFER_LENGTH", 512);
			DELIMITER_DELIMITERS_REGEX = getStringProperties("DataFormatter.DELIMITER_DELIMITERS_REGEX", "\\\\\\|");
		}

		/**
		 *  default character-encoder to be used
		 */
		public static String DEFAULT_CHARSET_ENCODER;// = "ISO-8859-1";
		/**
		 *  max length of field's value representation (bytes or characters).<br>
		 *  If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public static int FIELD_BUFFER_LENGTH;// = 512;

		/**
		 *  This regular expression is used by data parser when parsing
		 *  field delimiters out of metadata XML file.<br>
		 */
		 public static String DELIMITER_DELIMITERS_REGEX;// = "\\\\\\|";
	}


	/**
	 *  Defaults for various components
	 *
	 *@author     david
	 *@created    January 26, 2003
	 */
	public final static class Component {
		public static void init() {
			KEY_FIELDS_DELIMITER_REGEX = getStringProperties("Component.KEY_FIELDS_DELIMITER_REGEX", "\\s*[:;|]\\s*");
			KEY_FIELDS_DELIMITER = getStringProperties("Component.KEY_FIELDS_DELIMITER", ";");
		}

		/**
		 *  This regular expression is used by various components when parsing
		 *  parameters out of XML attributes.<br>
		 *  When attribute can contain multiple values delimited, this regex
		 *  specifies which are the valid delimiters.
		 */
		 public static String KEY_FIELDS_DELIMITER_REGEX;// = "\\s*[:;|]\\s*";
		 
		 /**
		  *  Delimiter character used when exporting components to XML
		  */
		 public static String KEY_FIELDS_DELIMITER;// = ";";
	}


	/**
	 *  Defaults for section Data
	 *
	 *@author     dpavlis
	 *@created    6. duben 2003
	 */
	public final static class Data {
		public static void init() {
			DATA_RECORDS_BUFFER_SIZE = getIntProperties("Data.DATA_RECORDS_BUFFER_SIZE", 10 * 1048576);
			MAX_BUFFERS_ALLOCATED = getShortProperties("Data.MAX_BUFFERS_ALLOCATED", (short) 99);
		}

		/**
		 *  Unit size of data buffer which keeps data records for sorting/hashing
		 */
		public static int DATA_RECORDS_BUFFER_SIZE;// = 10 * 1048576; // 10MB
		/**
		 *  How many units (buffers) can be allocated 
		 */
		public static short MAX_BUFFERS_ALLOCATED;// = 99;
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
		public static void init() {
			LOOKUP_INITIAL_CAPACITY = getIntProperties("Lookup.LOOKUP_INITIAL_CAPACITY", 512);
		}
	    
	    /**
	     * Initial size of lookup table (SimpleLookup)
	     */
	    public static int LOOKUP_INITIAL_CAPACITY;// = 512;
	    
	}
	
	public final static class WatchDog {
		public static void init() {
			WATCHDOG_SLEEP_INTERVAL = getIntProperties("WatchDog.WATCHDOG_SLEEP_INTERVAL", 500);
			DEFAULT_WATCHDOG_TRACKING_INTERVAL = getIntProperties("WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL", 30000);
			NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS = getIntProperties("WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS", 5);
		}

		/**
		 *  how long watchdog thread sleeps (milliseconds) between each awakening.
		 *
		 * @since    July 30, 2002
		 */
		
		public static int WATCHDOG_SLEEP_INTERVAL;// = 500;  	
		/**
		 *  how often is watchdog reporting about graph progress
		 *
		 * @since    July 30, 2002
		 */
		public static int DEFAULT_WATCHDOG_TRACKING_INTERVAL;// = 30000;

		/**
		 *  One tick is one awakening of watch dog. Sleep interval * number_of_ticks
		 *  determines how often is checked status of each component.<br>
		 *  If watchdog determines that there was an error in some component, the whole
		 *  graph processing is aborted.
		 *
		 * @since    October 1, 2002
		 */
		public static int NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;// = 5;

	}
	
	public final static class GraphProperties {
		public static void init() {
			PROPERTY_PLACEHOLDER_REGEX = getStringProperties("GraphProperties.PROPERTY_PLACEHOLDER_REGEX", "\\$\\{(\\w+)\\}");
		}
		
		public static String PROPERTY_PLACEHOLDER_REGEX;// = "\\$\\{(\\w+)\\}";
	}
}

