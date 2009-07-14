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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.zip.Deflater;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.string.StringUtils;

/**
 * Helper class which contains some framework-wide constants definitions.<br>
 * Change the compile-time defaults here !
 * 
 * @author dpavlis
 * @created January 23, 2003
 */
public final class Defaults {
	private static Properties properties;
	private static Log logger = LogFactory.getLog(Defaults.class);

	public static Properties getPropertiesSnapshot() {
		Properties p = new Properties();
		p.putAll(properties);
		return p;
	}

	private static void initProperties(String configurationFile) {
		InputStream is;
		
		//load defaults from build-in properties file as a java resource - defaultProperties
		is = openResourceStream("defaultProperties");
		if (is != null) {
			appendProperties(loadPropertiesFromStream(is, "defaultPropeties"));
		}

		//name of a resource file with properties can be also specify via 'cloveretl.properties' system property
		is = openResourceStream(System.getProperty("cloveretl.properties"));
		if (is != null) {
			appendProperties(loadPropertiesFromStream(is, "cloveretl.properties"));
		}
		
		//properties file name can be also passed as method parameter - configurationFile
		if (!StringUtils.isEmpty(configurationFile)) {
			try {
				logger.info("Loading Clover properties from file:"+configurationFile);
				appendProperties(loadPropertiesFromStream(new FileInputStream(configurationFile), configurationFile));
			} catch (FileNotFoundException e) {
				logger.warn("Unable to load properties from '" + configurationFile + "'.", e);
			}
		}
	}

	private static InputStream openResourceStream(String resourcename) {
		if (resourcename == null || resourcename.length() == 0) {
			return null;
		}

		InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream(resourcename);
		if (in == null) {
			in = Defaults.class.getResourceAsStream(resourcename);
		}

		if (in != null) {
			logger.info("Loading default properties from: " + resourcename);
		} else {
			logger.warn("Unable to load default properties from: " + resourcename);
		}

		return in;
	}

	private static Properties loadPropertiesFromStream(InputStream inputStream, String streamName) {
		Properties prop = new Properties();
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			logger.error("Unable to load properies stream: " + streamName, e);
			return prop;
		}
		try {
			inputStream.close();
		} catch (IOException e) {
			logger.warn("Unable to close properies stream: " + streamName);
		}
		return prop;
	}
	
	private static void appendProperties(Properties prop) {
		properties = new Properties(properties);
		properties.putAll(prop);
	}
	
	private static int getIntProperties(String key, int def) {
		String ret = getStringProperty(key);
		
		if (ret == null) {
			return def;
		}
		
		return Integer.parseInt(ret);
	}

	private static short getShortProperties(String key, short def) {
		String ret = getStringProperty(key);
		
		if (ret == null) {
			return def;
		}
		
		return Short.parseShort(ret);
	}

	private static String getStringProperties(String key, String def) {
		String ret = getStringProperty(key);
		
		if (ret == null) {
			return def;
		}
		
		return ret;
	}

	private static boolean getBooleanProperties(String key, boolean def) {
		String ret = getStringProperty(key);
		
		if (ret == null) {
			return def;
		}
		
		return Boolean.parseBoolean(ret);
	}

	/**
	 * Returns string form of property with the given key.
	 * A value from the system properties has higher priority then local properties.
	 */
	private static String getStringProperty(String key) {
		String ret = System.getProperty(key);
		
		if (ret == null) {
			ret = properties.getProperty(key);
		} else {
			//update local variable - it is neccesary for late invoking of getPropertiesSnapshot()
			properties.setProperty(key, ret);
		}

		return ret;
	}
	
	// public static void init() {
	// init(null);
	// }

	public static void init(String configurationFile) {
		initProperties(configurationFile);

		DEFAULT_INTERNAL_IO_BUFFER_SIZE = getIntProperties("DEFAULT_INTERNAL_IO_BUFFER_SIZE", 32768);
		DEFAULT_DATE_FORMAT = getStringProperties("DEFAULT_DATE_FORMAT", "yyyy-MM-dd");
		DEFAULT_TIME_FORMAT = getStringProperties("DEFAULT_TIME_FORMAT", "HH:mm:ss");
		DEFAULT_DATETIME_FORMAT = getStringProperties("DEFAULT_DATETIME_FORMAT", "yyyy-MM-dd HH:mm:ss");
		DEFAULT_REGEXP_TRUE_STRING = getStringProperties("DEFAULT_REGEXP_TRUE_STRING", "T|TRUE|YES|Y||t|true|1|yes|y");
		DEFAULT_REGEXP_FALSE_STRING = getStringProperties("DEFAULT_REGEXP_FALSE_STRING", "F|FALSE|NO|N||f|false|0|no|n");
		DEFAULT_LOCALE_STR_DELIMITER_REGEX = getStringProperties("DEFAULT_LOCALE_STR_DELIMITER_REGEX", "\\.");
		DEFAULT_BINARY_PATH = getStringProperties("DEFAULT_BINARY_PATH", "./bin/");
		DEFAULT_PATH_SEPARATOR_REGEX = getStringProperties("DEFAULT_FILENAME_SEPARATOR_REGEX", ";");
		DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE = getIntProperties("DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE", 2048);
		DEFAULT_PLUGINS_DIRECTORY = getStringProperties("DEFAULT_PLUGINS_DIRECTORY", "./plugins");
		CLOVER_FIELD_INDICATOR = getStringProperties("CLOVER_FIELD_INDICATOR", "$");
		CLOVER_FIELD_REGEX = getStringProperties("CLOVER_FIELD_REGEX", "\\$[\\w]+");
		ASSIGN_SIGN = getStringProperties("ASSIGN_SIGN", ":=");
		INCREMENTAL_STORE_KEY = getStringProperties("INCREMENTAL_STORE_KEY", "incremental_store");
		

		String compiler = getStringProperties("DEFAULT_JAVA_COMPILER", DynamicJavaCode.CompilerType.internal.name());
		try {
			DEFAULT_JAVA_COMPILER = DynamicJavaCode.CompilerType.valueOf(compiler);
		} catch (Exception e) {
			DEFAULT_JAVA_COMPILER = DynamicJavaCode.CompilerType.internal;
		}

		Record.init();
		DataFieldMetadata.init();
		DataParser.init();
		DataFormatter.init();
		Component.init();
		Data.init();
		Lookup.init();
		WatchDog.init();
		GraphProperties.init();
		InternalSortDataRecord.init();
		Graph.init();
		OracleConnection.init();
		TLCompiler.init();
	}

	/**
	 * when buffering IO, what is the default size of the buffer
	 */
	public static int DEFAULT_INTERNAL_IO_BUFFER_SIZE;// = 32768;

	/**
	 * Default path to external binary files.
	 */
	public static String DEFAULT_BINARY_PATH;// = "./bin/";

	/**
	 * Regex for separator of filenames in list of filenames - path separator.
	 */
	public static String DEFAULT_PATH_SEPARATOR_REGEX;// = ";";

	/**
	 * when creating InputStream or OutputStream objects, what is the size of their internal buffer. Used mainly in
	 * creating Channels from these streams.
	 */
	public static int DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE; // = 2048;

	/**
	 * when creating/parsing date from string, what is the expected/default format of date
	 */

	public static String DEFAULT_DATE_FORMAT;// = "yyyy-MM-dd";
	public static String DEFAULT_TIME_FORMAT;// = "HH:mm:ss";
	public static String DEFAULT_DATETIME_FORMAT;// = "yyyy-MM-dd HH:mm:ss";
	public static String DEFAULT_REGEXP_TRUE_STRING;// = "T|TRUE|YES|Y||t|true|1|yes|y"
	public static String DEFAULT_REGEXP_FALSE_STRING;// = "F|FALSE|NO|N||f|false|0|no|n"

	public static String DEFAULT_LOCALE_STR_DELIMITER_REGEX;// = "\\.";

	/**
	 * List of directories, where plugins are located. Paths separator is defined in DEFAULT_PATH_SEPARATOR_REGEX
	 * property.
	 */
	public static String DEFAULT_PLUGINS_DIRECTORY;// = "./plugins"

	/**
	 * Which java compiler implementation will be used for all inline java code.
	 */
	public static DynamicJavaCode.CompilerType DEFAULT_JAVA_COMPILER;

	/**
	 * string used for recognizing of clover field
	 */
	public static String CLOVER_FIELD_INDICATOR;// = "$";

	/**
	 * regex used for recognizing of clover field
	 */
	public static String CLOVER_FIELD_REGEX;// = \\$[\\w]+

	/**
	 * Assignation sign in the mappings
	 */
	public static String ASSIGN_SIGN;// = ":="

	/**
	 * the key name used for incremental reading if the pointer shouldn't be stored
	 */
	public static String INCREMENTAL_STORE_KEY;// = "incremental_action"

	
	/**
	 * Defaults regarding DataRecord structure/manipulation
	 * 
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class Record {
		public static void init() {
			MAX_RECORD_SIZE = getIntProperties("Record.MAX_RECORD_SIZE", 8192);
			DEFAULT_COMPRESSION_LEVEL = getIntProperties("Record.DEFAULT_COMPRESSION_LEVEL",
					Deflater.DEFAULT_COMPRESSION);
			USE_FIELDS_NULL_INDICATORS = getBooleanProperties("Record.USE_FIELDS_NULL_INDICATORS", false);
		}

		/**
		 * Determines max size of record (serialized) in bytes.<br>
		 * If you are getting BufferOverflow, increase the limit here. This affects the memory footprint !!!
		 */
		public static int MAX_RECORD_SIZE;// = 8192;
		/**
		 * Compression level for compressed data fields (CompressedByteField - cbyte). Should be set to a value from
		 * interval 0-9.
		 */
		public static int DEFAULT_COMPRESSION_LEVEL;// = Deflater.DEFAULT_COMPRESSION;

		/**
		 * Switch- shall we handle differently (during serialization) NULLable records (record which has at least one
		 * field NULLable ?)<br>
		 * If true then during serialization of record, first is saved array of bits (one bit for each field which can
		 * be NULLable) and bits are set depending of NULL status of the field being serialized.<br>
		 * This may speed serialization of record contains many fields with mostly NULL value assigned.
		 */
		public static boolean USE_FIELDS_NULL_INDICATORS; // = false;

	}

	/**
	 * Defaults regarding DataFieldMetadata
	 * 
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class DataFieldMetadata {
		public static void init() {
			DECIMAL_LENGTH = getIntProperties("DataFieldMetadata.DECIMAL_LENGTH", 10);
			DECIMAL_SCALE = getIntProperties("DataFieldMetadata.DECIMAL_SCALE", 2);
		}

		/**
		 * Determines default precision of decimal data field metatada. Example: <Field type="decimal" name="usrid"
		 * <b>length="10"</b> scale="2" delimiter=";" nullable="true" />
		 */
		public static int DECIMAL_LENGTH;// = 10;

		/**
		 * Determines default scale od decimal data field metadata.<br>
		 * Example: <Field type="decimal" name="usrid" length="10" <b>scale="2"</b> delimiter=";" nullable="true" />
		 */
		public static int DECIMAL_SCALE;// = 2;
	}

	/**
	 * Defaults for all DataParsers
	 * 
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class DataParser {
		public static void init() {
			FIELD_BUFFER_LENGTH = getIntProperties("DataParser.FIELD_BUFFER_LENGTH", 512);
			DEFAULT_CHARSET_DECODER = getStringProperties("DataParser.DEFAULT_CHARSET_DECODER", "ISO-8859-1");
		}

		/**
		 * max length of field's value representation (bytes or characters).<br>
		 * If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public static int FIELD_BUFFER_LENGTH;// = 512;

		/**
		 * default character-decoder to be used if not specified
		 */
		public static String DEFAULT_CHARSET_DECODER;// = "ISO-8859-1";
	}

	/**
	 * Defaults for DataFormatters
	 * 
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class DataFormatter {
		public static void init() {
			DEFAULT_CHARSET_ENCODER = getStringProperties("DataFormatter.DEFAULT_CHARSET_ENCODER", "ISO-8859-1");
			FIELD_BUFFER_LENGTH = getIntProperties("DataFormatter.FIELD_BUFFER_LENGTH", 512);
			DELIMITER_DELIMITERS_REGEX = getStringProperties("DataFormatter.DELIMITER_DELIMITERS_REGEX", "\\\\\\|");
		}

		/**
		 * default character-encoder to be used
		 */
		public static String DEFAULT_CHARSET_ENCODER;// = "ISO-8859-1";
		/**
		 * max length of field's value representation (bytes or characters).<br>
		 * If your records contain long fields (usually text-memos), increase the limit here.
		 */
		public static int FIELD_BUFFER_LENGTH;// = 512;

		/**
		 * This regular expression is used by data parser when parsing field delimiters out of metadata XML file.<br>
		 */
		public static String DELIMITER_DELIMITERS_REGEX;// = "\\\\\\|";
	}

	/**
	 * Defaults for various components
	 * 
	 * @author david
	 * @created January 26, 2003
	 */
	public final static class Component {
		public static void init() {
			KEY_FIELDS_DELIMITER_REGEX = getStringProperties("Component.KEY_FIELDS_DELIMITER_REGEX",
					"\\s*([|;]|:(?!=))\\s*");
			KEY_FIELDS_DELIMITER = getStringProperties("Component.KEY_FIELDS_DELIMITER", ";");
		}

		/**
		 * This regular expression is used by various components when parsing parameters out of XML attributes.<br>
		 * When attribute can contain multiple values delimited, this regex specifies which are the valid delimiters.
		 *  ; or | or : , but not := - this is ASSIGN_SIGN
		 */
		public static String KEY_FIELDS_DELIMITER_REGEX;// = "\\s*([|;]|:(?!=))\\s*"

		/**
		 * Delimiter character used when exporting components to XML
		 */
		public static String KEY_FIELDS_DELIMITER;// = ";";
	}

	/**
	 * Defaults for section Data
	 * 
	 * @author dpavlis
	 * @created 6. duben 2003
	 */
	public final static class Data {
		public static void init() {
			DATA_RECORDS_BUFFER_SIZE = getIntProperties("Data.DATA_RECORDS_BUFFER_SIZE", 10 * 1048576);
			MAX_BUFFERS_ALLOCATED = getShortProperties("Data.MAX_BUFFERS_ALLOCATED", (short) 99);
		}

		/**
		 * Unit size of data buffer which keeps data records for sorting/hashing
		 */
		public static int DATA_RECORDS_BUFFER_SIZE;// = 10 * 1048576; // 10MB
		/**
		 * How many units (buffers) can be allocated
		 */
		public static short MAX_BUFFERS_ALLOCATED;// = 99;
		// all together up to 990 MB
	}

	/**
	 * Defaults for lookup tables
	 * 
	 * @author david
	 * @since 25.3.2005
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
			WATCHDOG_SLEEP_INTERVAL = getIntProperties("WatchDog.WATCHDOG_SLEEP_INTERVAL", 1000);
			DEFAULT_WATCHDOG_TRACKING_INTERVAL = getIntProperties("WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL", 5000);
			NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS = getIntProperties("WatchDog.NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS",
					1);
		}

		/**
		 * how long watchdog thread sleeps (milliseconds) between each awakening.
		 * 
		 * @since July 30, 2002
		 */

		public static int WATCHDOG_SLEEP_INTERVAL;// = 1000;
		/**
		 * how often is watchdog reporting about graph progress
		 * 
		 * @since July 30, 2002
		 */
		public static int DEFAULT_WATCHDOG_TRACKING_INTERVAL;// = 5000;

		/**
		 * One tick is one awakening of watch dog. Sleep interval * number_of_ticks determines how often is checked
		 * status of each component.<br>
		 * If watchdog determines that there was an error in some component, the whole graph processing is aborted.
		 * 
		 * @since October 1, 2002
		 */
		public static int NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;// = 1;

	}

	public final static class GraphProperties {
		public static void init() {
			PROPERTY_PLACEHOLDER_REGEX = getStringProperties("GraphProperties.PROPERTY_PLACEHOLDER_REGEX",
					"\\$\\{(\\w+)\\}");
		}

		public static String PROPERTY_PLACEHOLDER_REGEX;// = "\\$\\{(\\w+)\\}";
	}

	public final static class InternalSortDataRecord {
		public static void init() {
			DEFAULT_INTERNAL_SORT_BUFFER_CAPACITY = getIntProperties(
					"InternalSortDataRecord.DEFAULT_INTERNAL_SORT_BUFFER_CAPACITY", 2000);
		}

		/**
		 * Size of internal buffer of internal record sorter. Specified in record count.
		 */
		public static int DEFAULT_INTERNAL_SORT_BUFFER_CAPACITY;
	}

	public final static class Graph {
		public static void init() {
			DIRECT_EDGE_INTERNAL_BUFFER_SIZE = getIntProperties("Graph.DIRECT_EDGE_INTERNAL_BUFFER_SIZE",
					Defaults.Record.MAX_RECORD_SIZE * 4);
			BUFFERED_EDGE_INTERNAL_BUFFER_SIZE = getIntProperties("Graph.BUFFERED_EDGE_INTERNAL_BUFFER_SIZE",
					Defaults.Record.MAX_RECORD_SIZE * 10);
			DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS = getIntProperties(
					"Graph.DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS", 4);
		}

		/**
		 * Size of internal buffer of DirectEdge for storing data records when transmitted between two components. The
		 * size should be at least MAX_RECORD_SIZE + 8, better several times bigger
		 */
		public static int DIRECT_EDGE_INTERNAL_BUFFER_SIZE;

		/**
		 * Size of internal buffer of BufferedEdge for storing/caching data records. BufferedEdge is used when engine
		 * needs to compensate fact that component reads data from two different ports and there might be some
		 * interdependencies between the source data flows. The size should be at least
		 * Defaults.Record.MAX_RECORD_SIZE*10, better several times bigger - 128kB or more
		 */
		public static int BUFFERED_EDGE_INTERNAL_BUFFER_SIZE;

		/**
		 * Number of internal buffers for storing/buffering records transmitted through FastPropagate Edge. One buffer
		 * can store one data record. Minimum size is 1. Default is 4. Higher number can help increasing processing
		 * speed but not much.
		 */
		public static int DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS;
	}
	
	public final static class OracleConnection {
		public static void init() {
			ROW_PREFETCH = getIntProperties("OracleConnection.ROW_PREFETCH", -1);
		}

		/**
		 * The number of rows of data that are fetched each time data is fetched;
		 * the extra data is stored in client-side buffers for later access by the client.
		 */
		public static int ROW_PREFETCH; // -1
	}
	
	
	/**
	 * Default settings for CTL language compilers.
	 * 
	 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
	 *
	 */
	public static final class TLCompiler {
		public static void init() {
			TLCOMPILER_SOURCE_DIRECTORY = getStringProperties("TLCompiler.TLCOMPILER_SOURCE_DIRECTORY",
					"./trans");
			TLCOMPILER_BINARY_DIRECTORY = getStringProperties("TLCompiler.TLCOMPILER_BINARY_DIRECTORY",
			"./trans");

		}
		
		/**
		 * Directory where all generated .java source files are stored
		 */
		public static String TLCOMPILER_SOURCE_DIRECTORY;
		
		/**
		 * Directory where all generated .class files are stored
		 */
		public static String TLCOMPILER_BINARY_DIRECTORY;
	}
	
	
}
