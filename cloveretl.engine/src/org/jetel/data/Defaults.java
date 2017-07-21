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
package org.jetel.data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.Deflater;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.MemoryUtils;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTimeZone;

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

	/**
	 * Returns snapshot of engine properties.
	 * If the engine properties are stored as separated properties and defaults (@see java.util.Properties), this method returns merged properties.
	 */
	public static Properties getPropertiesSnapshot() {
		Properties p = new Properties();
		Set<String> keys = properties.stringPropertyNames();
		// must be implemented in this complex way, otherwise Properties.defaults would be ignored
		for (String key : keys) {
			String v = properties.getProperty(key); // must be getProperty, otherwise Properties.defaults would be ignored 
			if (v != null) {
				p.put(key, v);
			} 
		}// for
		return p;
	}

    private static void initProperties(URL configurationFile) {
        loadDefaultProperties();

        //properties file name can be also passed as method parameter - configurationFile
        if (configurationFile != null) {
            try {
                logger.info("Loading Clover properties from file:" + configurationFile);
                appendProperties(loadPropertiesFromStream(configurationFile.openStream(), configurationFile.toString()));
            } catch (IOException e) {
                logger.warn("Unable to load properties from '" + configurationFile + "'.", e);
            }
        }
    }

	private static void initProperties(String configurationFile) {
		loadDefaultProperties();

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

    private static void loadDefaultProperties() {
    	Defaults.properties = null; // prevent memory leak caused by multiple initialization
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

	private static long getLongProperties(String key, long def) {
		String ret = getStringProperty(key);

		if (ret == null) {
			return def;
		}

		return Long.parseLong(ret);
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

    public static void init() {
        loadDefaultProperties();
        initializeInternal();
    }

	public static void init(String configurationFile) {
		initProperties(configurationFile);
		initializeInternal();
	}

    public static void init(URL configurationFile) {
        initProperties(configurationFile);
        initializeInternal();
    }

    private static void initializeInternal() {
        DEFAULT_INTERNAL_IO_BUFFER_SIZE = getIntProperties("DEFAULT_INTERNAL_IO_BUFFER_SIZE", 32768);
        DEFAULT_DATE_FORMAT = getStringProperties("DEFAULT_DATE_FORMAT", "yyyy-MM-dd");
        DEFAULT_TIME_FORMAT = getStringProperties("DEFAULT_TIME_FORMAT", "HH:mm:ss");
        DEFAULT_DATETIME_FORMAT = getStringProperties("DEFAULT_DATETIME_FORMAT", "yyyy-MM-dd HH:mm:ss");
        DEFAULT_LOCALE = getStringProperties("DEFAULT_LOCALE", MiscUtils.localeToString(Locale.getDefault()));
        DEFAULT_TIME_ZONE = getStringProperties("DEFAULT_TIME_ZONE", "'java:"+ TimeZone.getDefault().getID() + "';'joda:" + DateTimeZone.getDefault().getID() + "'");
        DEFAULT_REGEXP_TRUE_STRING = getStringProperties("DEFAULT_REGEXP_TRUE_STRING", "T|TRUE|YES|Y||t|true|1|yes|y");
        DEFAULT_REGEXP_FALSE_STRING = getStringProperties("DEFAULT_REGEXP_FALSE_STRING", "F|FALSE|NO|N||f|false|0|no|n");
        DEFAULT_BINARY_PATH = getStringProperties("DEFAULT_BINARY_PATH", "./bin/");
        DEFAULT_SOURCE_CODE_CHARSET = getStringProperties("DEFAULT_SOURCE_CODE_CHARSET", "UTF-8");
        DEFAULT_PATH_SEPARATOR_REGEX = getStringProperties("DEFAULT_FILENAME_SEPARATOR_REGEX", ";");
        DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE = getIntProperties("DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE", 2048);
        DEFAULT_PLUGINS_DIRECTORY = getStringProperties("DEFAULT_PLUGINS_DIRECTORY", "./plugins");
		DEFAULT_LICENSE_LOCATION = getStringProperties("DEFAULT_LICENSE_DIRECTORY", "./licenses");
        CLOVER_FIELD_INDICATOR = getStringProperties("CLOVER_FIELD_INDICATOR", "$");
        CLOVER_FIELD_REGEX = getStringProperties("CLOVER_FIELD_REGEX", "\\$[\\w]+");
        ASSIGN_SIGN = getStringProperties("ASSIGN_SIGN", ":=");
        INCREMENTAL_STORE_KEY = getStringProperties("INCREMENTAL_STORE_KEY", "incremental_store");
        PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING = getStringProperties("PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING", "java.;javax.;sun.misc.");
        USE_DIRECT_MEMORY = getBooleanProperties("USE_DIRECT_MEMORY", true);
        USE_DYNAMIC_COMPILER = getBooleanProperties("USE_DYNAMIC_COMPILER", true);
        MAX_MAPPED_FILE_TRANSFER_SIZE = getIntProperties("MAX_MAPPED_FILE_TRANSFER_SIZE", 8388608);
        CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE = getLongProperties("CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE", MemoryUtils.getDirectMemorySize() / 2);
        
        
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
        CTL.init();
        PortReadingWriting.init();
        ConnectionPool.init();
    }

	/**
	 * when buffering IO, what is the default size of the buffer
	 */
	public static int DEFAULT_INTERNAL_IO_BUFFER_SIZE;// = 32768;
	
	/**
	 * Used in FileChannel.transferFrom() and FileChannel.transferTo() 
	 */
	public static int MAX_MAPPED_FILE_TRANSFER_SIZE; // = 8 MB;

	/**
	 * Default path to external binary files.
	 */
	public static String DEFAULT_BINARY_PATH;// = "./bin/";

	/**
	 * Default charset used when parsing source code (CTL or Java)
	 */
	public static String DEFAULT_SOURCE_CODE_CHARSET;// = "UTF-8";
	/**
	 * Regex for separator of filenames in list of filenames - path separator.
	 */
	public static String DEFAULT_PATH_SEPARATOR_REGEX;// = ";";

	/**
	 * when creating InputStream or OutputStream objects, what is the size of their internal buffer. Used mainly in
	 * creating Channels from these streams.
	 * @deprecated use {@link #DEFAULT_INTERNAL_IO_BUFFER_SIZE} instead
	 */
	@Deprecated
	public static int DEFAULT_IOSTREAM_CHANNEL_BUFFER_SIZE; // = 2048;

	/**
	 * when creating/parsing date from string, what is the expected/default format of date
	 */

	public static String DEFAULT_DATE_FORMAT;// = "yyyy-MM-dd";
	public static String DEFAULT_TIME_FORMAT;// = "HH:mm:ss";
	public static String DEFAULT_DATETIME_FORMAT;// = "yyyy-MM-dd HH:mm:ss";
	public static String DEFAULT_LOCALE;// = MiscUtils.localeToString(Locale.getDefault());
	public static String DEFAULT_TIME_ZONE;// = TimeZone.getDefault().getID();
	public static String DEFAULT_REGEXP_TRUE_STRING;// = "T|TRUE|YES|Y|t|true|1|yes|y"
	public static String DEFAULT_REGEXP_FALSE_STRING;// = "F|FALSE|NO|N|f|false|0|no|n"

	/**
	 * List of directories, where plugins are located. Paths separator is defined in DEFAULT_PATH_SEPARATOR_REGEX
	 * property.
	 */
	public static String DEFAULT_PLUGINS_DIRECTORY;// = "./plugins"

	/**
	 * List of directories, where licenses are located. Paths separator is defined in DEFAULT_PATH_SEPARATOR_REGEX
	 * property.
	 */
	public static String DEFAULT_LICENSE_LOCATION;// = "./licenses"
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
	 * List of package prefixes which are excluded from greedy class loading. i.e. "java." "javax." "sun.misc." etc.
	 * Prevents GreedyClassLoader from loading interfaces and common classes from external libs.
	 * */
	public static String PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING;

	/**
	 * Clover engine intensively uses direct memory for data records manipulation.
	 * For example underlying memory of {@link CloverBuffer} (serialised data records container)
	 * is allocated outside the Java heap space in direct memory.
	 * This attribute is <code>true</code> by default due better performance.
	 * Since direct memory is out of control java virtual machine, try to turn off 
	 * usage of direct memory in case OutOfMemory exception occurs. 
	 */
	public static boolean USE_DIRECT_MEMORY;// = true;
	
	/**
	 * Clover engine can use dynamic compiler functionality for runtime compilation
	 * of user-defined java code, for example transformation of Reformat component can
	 * be specified by a java code and this code is automatically compiled be engine and
	 * used for records transformation. Also 'compiled' mode of CLT2 code is actually backed
	 * by dynamic compilation of java code. This functionality is powerful
	 * but potential security issue. Setting this attribute to false, administrator can
	 * turn off dynamic compiler at all.
	 */
	public static boolean USE_DYNAMIC_COMPILER;// = true;
	
	/**
	 * Maximal size of direct memory used by clover buffers. By default,
	 * only half of all direct memory can be used for {@link CloverBuffer} instances.
	 * WARN: MemoryUtils.getDirectMemorySize() actually returns size of heap, since real 
	 * size of direct memory is not available
	 */
	public static long CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE;// = MemoryUtils.getDirectMemorySize() / 2

	/**
	 * Defaults regarding DataRecord structure/manipulation
	 *
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class Record {
		public static void init() {
			RECORD_INITIAL_SIZE = getIntProperties("Record.RECORD_INITIAL_SIZE", 65536);
			RECORD_LIMIT_SIZE = getIntProperties("Record.RECORD_LIMIT_SIZE", 33554432);
			MAX_RECORD_SIZE = getIntProperties("Record.MAX_RECORD_SIZE", 65536);
			FIELD_INITIAL_SIZE = getIntProperties("Record.FIELD_INITIAL_SIZE", 65536);
			FIELD_LIMIT_SIZE = getIntProperties("Record.FIELD_LIMIT_SIZE", 33554432);
			DEFAULT_COMPRESSION_LEVEL = getIntProperties("Record.DEFAULT_COMPRESSION_LEVEL",
					Deflater.DEFAULT_COMPRESSION);
			USE_FIELDS_NULL_INDICATORS = getBooleanProperties("Record.USE_FIELDS_NULL_INDICATORS", false);
			RECORDS_BUFFER_SIZE = getIntProperties("Graph.RECORDS_BUFFER_SIZE",
					Defaults.Record.RECORD_INITIAL_SIZE * 4);
		}

		/**
		 * Should be used as initial size of CloverBuffer dedicated to handle a data record.
		 * Determines expected upper bounds of record size (serialized) in bytes.<br>
		 * Clover engine is able to handle even bigger records. Underlying buffer
		 * may be re-allocated in the case a bigger record appears.
		 */
		public static int RECORD_INITIAL_SIZE;// = 65536;

		/**
		 * Determines maximum size of record (serialized) in bytes.<br>
		 * If you are getting BufferOverflow, increase the limit here.
		 */
		public static int RECORD_LIMIT_SIZE;// = 33554432;

		/**
		 * Obsolete constant, which was substituted by logicaly same constant {@link #RECORD_LIMIT_SIZE}.
		 * In case this is used as buffer size of a ByteBuffer (container for serialized form of a data record)
		 * {@link #RECORD_INITIAL_SIZE} should be used.
		 * @deprecated use {@link #RECORD_INITIAL_SIZE} or {@link #RECORD_LIMIT_SIZE} instead
		 */
		@Deprecated
		public static int MAX_RECORD_SIZE;// = 65536;

		/**
		 * Should be used as initial size of CloverBuffer dedicated to handle a data field.
		 * Determines expected upper bounds of field size (serialized) in bytes.<br>
		 * Clover engine is able to handle even bigger fields. Underlying buffer
		 * may be re-allocated in the case a bigger field appears.
		 */
		public static int FIELD_INITIAL_SIZE;// = 65536;

		/**
		 * Determines maximum size of single field (serialized) in bytes.<br>
		 * If you are getting BufferOverflow, increase the limit here.
		 */
		public static int FIELD_LIMIT_SIZE;// = 33554432;

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

		/**
		 * Size of internal buffer for temporary storing several data records.
		 * Size should be at least {@link #RECORD_INITIAL_SIZE}, better several times bigger
		 */
		public static int RECORDS_BUFFER_SIZE;

	}

	/**
	 * Defaults regarding DataFieldMetadata
	 *
	 * @author dpavlis
	 * @created January 23, 2003
	 */
	public final static class DataFieldMetadata {
		public static void init() {
			DECIMAL_LENGTH = getIntProperties("DataFieldMetadata.DECIMAL_LENGTH", 12);
			DECIMAL_SCALE = getIntProperties("DataFieldMetadata.DECIMAL_SCALE", 2);
		}

		/**
		 * Determines default precision of decimal data field metatada. Example: <Field type="decimal" name="usrid"
		 * <b>length="10"</b> scale="2" delimiter=";" nullable="true" />
		 */
		public static int DECIMAL_LENGTH;// = 12;

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
		 * @deprecated use {@link Record#FIELD_INITIAL_SIZE} instead
		 */
		@Deprecated
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
		 * @deprecated use {@link Record#FIELD_INITIAL_SIZE} instead
		 */
		@Deprecated
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
			StringDataField.init();
			DATA_RECORDS_BUFFER_SIZE = getIntProperties("Data.DATA_RECORDS_BUFFER_SIZE", 10 * 1048576);
			MAX_BUFFERS_ALLOCATED = getShortProperties("Data.MAX_BUFFERS_ALLOCATED", (short) 99);
		}

		public final static class StringDataField {
			public static void init() {
				DIRECT_BULK_SERIALIZATION_THRESHOLD = getIntProperties("Data.StringDataField.DIRECT_BULK_SERIALIZATION_THRESHOLD", 130);
				DIRECT_BULK_DESERIALIZATION_THRESHOLD = getIntProperties("Data.StringDataField.DIRECT_BULK_DESERIALIZATION_THRESHOLD", 20);
				NON_DIRECT_BULK_SERIALIZATION_THRESHOLD = getIntProperties("Data.StringDataField.NON_DIRECT_BULK_SERIALIZATION_THRESHOLD", 6);
				NON_DIRECT_BULK_DESERIALIZATION_THRESHOLD = getIntProperties("Data.StringDataField.NON_DIRECT_BULK_DESERIALIZATION_THRESHOLD", 6);
			}

			public static int DIRECT_BULK_SERIALIZATION_THRESHOLD;// 130

			public static int DIRECT_BULK_DESERIALIZATION_THRESHOLD;// 20

			public static int NON_DIRECT_BULK_SERIALIZATION_THRESHOLD;// 6

			public static int NON_DIRECT_BULK_DESERIALIZATION_THRESHOLD;// 6
		}

		/**
		 * Unit size of data buffer which keeps data records for sorting/hashing
		 * @deprecated invalidated without substitution, consider to use {@link Record#RECORDS_BUFFER_SIZE}
		 */
		@Deprecated
		public static int DATA_RECORDS_BUFFER_SIZE;// = 10 * 1048576; // 10MB

		/**
		 * How many units (buffers) can be allocated
		 * @deprecated invalidated without substitution
		 */
		@Deprecated
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
			EXPRESSION_EVALUATION_ENABLED = getBooleanProperties(
					"GraphProperties.EXPRESSION_EVALUATION_ENABLED", true);

			EXPRESSION_PLACEHOLDER_REGEX = getStringProperties(
					"GraphProperties.EXPRESSION_PLACEHOLDER_REGEX", "(?<!\\\\)`(.*?)(?<!\\\\)`");
			PROPERTY_PLACEHOLDER_REGEX = getStringProperties(
					"GraphProperties.PROPERTY_PLACEHOLDER_REGEX", "\\$\\{(\\w+)\\}");
			PROPERTY_ALLOWED_RECURSION_DEPTH = getIntProperties(
					"GraphProperties.PROPERTY_ALLOWED_RECURSION_DEPTH", 1000);
		}

		/** determines whether the CTL expressions within properties should be evaluated or not */
		public static boolean EXPRESSION_EVALUATION_ENABLED;// = true

		/** a regular expression describing the format of CTL expressions */
		public static String EXPRESSION_PLACEHOLDER_REGEX;// = "(?<!\\\\)`(.*?)(?<!\\\\)`";
		/** a regular expression describing the format of property references */
		public static String PROPERTY_PLACEHOLDER_REGEX;// = "\\$\\{([a-zA-Z_]\\w*)\\}";

		/** allowed depth of recursion by graph property resolving */
		public static int PROPERTY_ALLOWED_RECURSION_DEPTH;// = 1000;

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
					Defaults.Record.RECORD_INITIAL_SIZE * 4);
			BUFFERED_EDGE_INTERNAL_BUFFER_SIZE = getIntProperties("Graph.BUFFERED_EDGE_INTERNAL_BUFFER_SIZE",
					Defaults.Record.RECORD_INITIAL_SIZE * 10);
			DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS = getIntProperties(
					"Graph.DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS", 4);
		}

		/**
		 * Size of internal buffer of DirectEdge for storing data records when transmitted between two components. The
		 * size should be at least {@link Record#RECORD_INITIAL_SIZE}, better several times bigger
		 */
		public static int DIRECT_EDGE_INTERNAL_BUFFER_SIZE;

		/**
		 * Size of internal buffer of BufferedEdge for storing/caching data records. BufferedEdge is used when engine
		 * needs to compensate fact that component reads data from two different ports and there might be some
		 * interdependencies between the source data flows. The size should be at least
		 * {@link Record#RECORD_INITIAL_SIZE}, better several times bigger - 128kB or more
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
	
	public static final class DBConnection {
		
		public static void init() {
			VALIDATION_TIMEOUT = getIntProperties("DBConnection.VALIDATION_TIMEOUT", 30);
		}
		
		/**
		 * Number of seconds for JDBC connection validation in DBConnection, see 
		 * {@link Connection#isValid(int)}.
		 */
		public static int VALIDATION_TIMEOUT; // 30
	}

	/**
	 * Default settings for CTL.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 */
	public static final class CTL {

		public static void init() {
			VOID_METADATA_NAME = getStringProperties("TLCompiler.VOID_METADATA_NAME", "VoidMetadata");
			DECIMAL_PRECISION = getIntProperties("CTL.DECIMAL_PRECISION", 32);
		}

		/** The name of void metadata used by Rollup transforms when no group accumulator is used. */
		public static String VOID_METADATA_NAME;
		
		/**
		 * Applies for decimal division and double to decimal assignment.
		 * @see org.jetel.ctl.TransformLangExecutor.DECIMAL_MAX_PRECISION
		 */
		public static int DECIMAL_PRECISION; // = 32

	}

	/**
	 * Defaults for port reading/writing - stream mode.
	 *
	 * @author jausperger
	 * @created September 10, 2009
	 */
	public final static class PortReadingWriting {
		public static void init() {
			DATA_LENGTH = getIntProperties("PortReadingWriting.DATA_LENGTH", 2048);
		}

		/**
		 * It determines what is the maximum size of one particular data field for stream mode for writer components.
		 * This value must be less or equal to similar field or record buffers.
		 */
		public static int DATA_LENGTH;// = 2048;

	}
	
	/**
	 * Defaults for file operations' connection pool.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Feb 15, 2013
	 */
	public static final class ConnectionPool {
		public static void init() {
			MAX_IDLE_TIME = getLongProperties("ConnectionPool.MAX_IDLE_TIME", 1 * 60 * 1000L);
			CLEANUP_INTERVAL = getLongProperties("ConnectionPool.CLEANUP_INTERVAL", 1 * 60 * 1000L);
		}
		
		/**
		 * If the connection has been sitting idle in the pool
		 * longer than <code>MAX_IDLE_TIME</code>,
		 * it will be removed by the next cleanup.
		 */
		public static long MAX_IDLE_TIME; // 1 * 60 * 1000L (1 minute)
		
		/**
		 * How often will idle connections be removed from the pool.
		 * 
		 * If set to a negative number, 
		 * idle connections will be kept forever and will only be removed
		 * if they fail the test on borrow 
		 * (i.e. if the underlying connection timeouts).
		 */
		public static long CLEANUP_INTERVAL; // 1 * 60 * 1000L (1 minute)
	}

}
