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
package org.jetel.ctl.extensions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.jetel.ctl.Stack;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.util.MiscUtils;
import org.jetel.util.Pair;
import org.jetel.util.formatter.TimeZoneProvider;
import org.jetel.util.primitive.TypedProperties;

/**
 * Additional functions for dynamic field access.
 * 
 * @author Branislav Repcek (branislav.repcek@javlin.eu)
 * @author krivanekm
 */
public class DynamicLibExt extends TLFunctionLibraryExt {

	private static final String LIBRARY_NAME = "DynamicLib";
	
	private static final Set<String> EXCLUDED_RECORD_PROPERTIES;
	
	static {
		List<String> initializer = Arrays.asList("previewAttachment", "previewAttachmentCharset", "previewAttachmentMetadataRow", "previewAttachmentSampleDataRow");
		EXCLUDED_RECORD_PROPERTIES = new HashSet<String>(initializer.size());
		EXCLUDED_RECORD_PROPERTIES.addAll(initializer);
	}

	public DynamicLibExt() {
		super(LIBRARY_NAME);
	}
	
	/**
	 * Query field properties of given field. These properties can be configured in metadata editor and are therefore
	 * static while the graph is running. Internal cache is used to speed up the queries for repeated queries of the
	 * same field.
	 * 
	 * @param context function call context.
	 * @param record record to which the queried field belongs. Cannot be null.
	 * @param fieldName name of the field to query. Cannot be null. Note that field names are case sensitive.
	 * 
	 * @return map which maps the name of the field property to its value. All property values are converted to strings.
	 *         If no properties are declared, returns empty map. Note that property value can be null.
	 */
	@TLFunctionAnnotation("Return field properties configured in metadata editor.")
	@CTL2FunctionDeclaration(impl = GetFieldPropertiesFunction.class)
	public static Map< String, String > getFieldProperties(TLFunctionCallContext context, DataRecord record, String fieldName) {
		
		if (record == null) {
			throw new NullPointerException("Input record cannot be null.");
		}
		
		if (fieldName == null) {
			throw new NullPointerException("Field name cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		int fieldIndex = recordMeta.getFieldPosition(fieldName);
		if (fieldIndex < 0) {
			throw new IllegalArgumentException("Field '" + fieldName + "' not found in metadata '" + recordMeta.getName() + "'.");
		}
		
		FieldPropertyCache cache = (FieldPropertyCache) context.getCache();
		
		return cache.getFieldProperties(recordMeta, fieldIndex);
	}
	
	/**
	 * Query field properties of given field. These properties can be configured in metadata editor and are therefore
	 * static while the graph is running. Internal cache is used to speed up the queries for repeated queries of the
	 * same field.
	 * 
	 * @param context function call context.
	 * @param record record to which the queried field belongs. Cannot be null.
	 * @param fieldIndex index of the field to query. Index has to be greater than zero and less than number of fields
	 *        in the record. First field has index of zero.
	 * 
	 * @return map which maps the name of the field property to its value. All property values are converted to strings.
	 *         If no properties are declared, returns empty map. Note that property value can be null.
	 */
	@TLFunctionAnnotation("Return field properties configured in metadata editor.")
	@CTL2FunctionDeclaration(impl = GetFieldPropertiesFunction.class)
	public static Map< String, String > getFieldProperties(TLFunctionCallContext context, DataRecord record, Integer fieldIndex) {

		if (record == null) {
			throw new NullPointerException("Input record cannot be null.");
		}
		
		if (fieldIndex == null) {
			throw new NullPointerException("Field index cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		if (fieldIndex < 0 || fieldIndex >= recordMeta.getNumFields()) {
			throw new IndexOutOfBoundsException("Field index out of bounds.");
		}
		
		FieldPropertyCache cache = (FieldPropertyCache) context.getCache();
		
		return cache.getFieldProperties(recordMeta, fieldIndex);
	}
	
	@TLFunctionInitAnnotation
	public static void getFieldPropertiesInit(TLFunctionCallContext context) {
		context.setCache(context.getSharedInstance(FieldPropertyCache.class));
	}
	
	/**
	 * Query properties of the given record. These properties can be configured in metadata editor and are therefore
	 * static while the graph is running. Internal cache is used to speed up the queries for repeated queries of the
	 * same record.
	 * 
	 * @param context function call context.
	 * @param record record to query. Cannot be null.
	 * 
	 * @return map which maps the name of the record property to its value. All property values are converted to strings.
	 *         If no properties are declared, returns empty map. Note that property value can be null.
	 */
	@TLFunctionAnnotation("Query record properties configured in the Metadata editor.")
	@CTL2FunctionDeclaration(impl = GetRecordPropertiesFunction.class)
	public static Map< String, String > getRecordProperties(TLFunctionCallContext context, DataRecord record) {
		
		if (record == null) {
			throw new NullPointerException("Input record cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		RecordPropertyCache cache = (RecordPropertyCache) context.getCache();
		
		return cache.getRecordProperties(recordMeta);
	}

	@TLFunctionInitAnnotation
	public static void getRecordPropertiesInit(TLFunctionCallContext context) {
		context.setCache(context.getSharedInstance(RecordPropertyCache.class));
	}
	
	public static class GetFieldPropertiesFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: (fieldName | fieldIndex), record
			Object fieldId = stack.pop();
			DataRecord record = stack.popRecord();
			
			TLType type = context.getParams()[1];
			
			if (type.isString()) {
				stack.push(getFieldProperties(context, record, (String) fieldId));
			} else if (type.isInteger()) {
				stack.push(getFieldProperties(context, record, (Integer) fieldId));
			} else {
				throw new IllegalArgumentException("Unsupported parameter type: " + fieldId.getClass().getCanonicalName());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
			getFieldPropertiesInit(context);
		}
	}
	
	public static class GetRecordPropertiesFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: record
			stack.push(getRecordProperties(context, stack.popRecord()));
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
			getRecordPropertiesInit(context);
		}
	}

	/**
	 * <p>Cache which stores converted record properties so that multiple queries for the same property map do not result
	 * in complicated and costly conversion.</p>
	 * <p>Cache is initialized lazily - cache entries are added when they are queried for the first time.</p>
	 * 
	 * @author Branislav Repcek (branislav.repcek@javlin.eu)
	 */
	public static class RecordPropertyCache extends TLCache {

		// identity-based comparison should be used, otherwise metadata with equal structure but different names are considered equal
		private Map< DataRecordMetadata, Map< String, String > > recordPropertyCache = 
				new IdentityHashMap<>();
	
		/**
		 * Get record properties. Uses cache to keep the properties of previously queried metadata to save the expensive
		 * data conversions.
		 * 
		 * @param metadata record metadata. Cannot be null.
		 * 
		 * @return map of all properties and their values. Property values are converted to string. Never returns null -
		 *         empty property list results in empty map.
		 */
		public Map< String, String > getRecordProperties(DataRecordMetadata metadata) {

			Map< String, String > properties = recordPropertyCache.get(metadata);
			
			if (properties != null) {
				return properties;
			}
			
			properties = new TreeMap< String, String >();
			
			properties.put(DataRecordMetadataXMLReaderWriter.NAME_ATTR, metadata.getName());
			properties.put(DataRecordMetadataXMLReaderWriter.LABEL_ATTR, metadata.getLabelOrName());
			properties.put(DataRecordMetadataXMLReaderWriter.TYPE_ATTR, DataRecordMetadataXMLReaderWriter.toString(metadata.getParsingType()));
			properties.put(DataRecordMetadataXMLReaderWriter.DESCRIPTION_ATTR, metadata.getDescription());
			
			properties.put(DataRecordMetadataXMLReaderWriter.RECORD_DELIMITER_ATTR, metadata.getRecordDelimiter());
			properties.put(DataRecordMetadataXMLReaderWriter.FIELD_DELIMITER_ATTR, metadata.getFieldDelimiter());
			
			properties.put(DataRecordMetadataXMLReaderWriter.QUOTED_STRINGS, Boolean.toString(metadata.isQuotedStrings()));
			properties.put(DataRecordMetadataXMLReaderWriter.QUOTE_CHAR, DynamicLibExt.toString(metadata.getQuoteChar()));
			
			properties.put(DataRecordMetadataXMLReaderWriter.LOCALE_ATTR, MiscUtils.localeToString(MiscUtils.createLocale(metadata.getLocaleStr()))); // CLO-6293
			properties.put(DataRecordMetadataXMLReaderWriter.TIMEZONE_ATTR, new TimeZoneProvider(metadata.getTimeZoneStr()).toString()); // CLO-6293
			properties.put(DataRecordMetadataXMLReaderWriter.NULL_VALUE_ATTR, metadata.getNullValue());
			
			properties.put(DataRecordMetadataXMLReaderWriter.EOF_AS_DELIMITER_ATTR, Boolean.toString(metadata.isEofAsDelimiter()));

			// add custom properties
			TypedProperties p = metadata.getRecordProperties();
			for (String s: p.stringPropertyNames()) {
				if (!EXCLUDED_RECORD_PROPERTIES.contains(s)) { // CLO-6293
					properties.put(s, p.getStringProperty(s));
				}
			}

			properties = Collections.unmodifiableMap(new LinkedHashMap< String, String >(properties));
			
			// Store the value in the cache for next time.
			recordPropertyCache.put(metadata, properties);

			return properties;
		}
	}
	
	/**
	 * Overridden to use identity for metadata comparison.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 7. 4. 2015
	 */
	private static class DataFieldWrapper extends Pair<DataRecordMetadata, Integer> {

		/**
		 * @param metadata
		 * @param idx
		 */
		public DataFieldWrapper(DataRecordMetadata metadata, Integer idx) {
			super(metadata, idx);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if ((o == null) || (getClass() != o.getClass())) {
				return false;
			}
			
			DataFieldWrapper p = (DataFieldWrapper) o;
			return (this.first == p.first) && Objects.equals(this.second, p.second);
		}

		@Override
		public int hashCode() {
			int result = 17;
			result = 31 * result + System.identityHashCode(first);
			result = 31 * result + Objects.hashCode(second);
			return result;
		}

	}
	
	/**
	 * <p>Cache which stores converted field properties so that multiple queries for the same property map do not result
	 * in complicated and costly conversion.</p>
	 * <p>Cache is initialized lazily - cache entries are added when they are queried for the first time.</p>
	 * 
	 * @author Branislav Repcek (branislav.repcek@javlin.eu)
	 */
	public static class FieldPropertyCache extends TLCache {
		
		/**
		 * Cache for resolved and converted field properties. Key is pair<metadata, fieldName> while the value is
		 * map<fieldPropName, fieldPropValue>.
		 */
		private Map< DataFieldWrapper, Map< String, String > > fieldPropertyCache = new HashMap<>();
		
		/**
		 * Get field property map for given metadata field. If the field has been queried before, the value is returned
		 * from cache. Otherwise new cache entry is created and stored in the cache. The cache never expires.
		 * 
		 * @param metadata metadata which contains the queried field.
		 * @param fieldName name of the field to query. Cannot be null, case sensitive.
		 * 
		 * @return map of all properties of given field. If no properties are defined, returns empty map. Never returns null.
		 */
		public Map< String, String > getFieldProperties(DataRecordMetadata metadata, int idx) {
			
			DataFieldWrapper key = new DataFieldWrapper(metadata, idx);
			Map< String, String > properties = fieldPropertyCache.get(key);

			if (properties != null) {
				return properties;
			}

			// Not in the cache, so build the cache entry
			properties = new TreeMap< String, String >();
			
			DataFieldMetadata field = metadata.getField(idx);
			
			properties.put(DataRecordMetadataXMLReaderWriter.NAME_ATTR, field.getName());
			properties.put(DataRecordMetadataXMLReaderWriter.LABEL_ATTR, field.getLabelOrName());
			properties.put(DataRecordMetadataXMLReaderWriter.TYPE_ATTR, field.getDataType().getName());
			DataFieldContainerType containerType = field.getContainerType();
			properties.put(DataRecordMetadataXMLReaderWriter.CONTAINER_TYPE_ATTR, containerType != DataFieldContainerType.SINGLE ? containerType.getDisplayName() : null);
			properties.put(DataFieldMetadata.LENGTH_ATTR, null);
			properties.put(DataFieldMetadata.SCALE_ATTR, null);
			if (field.isDelimited()) {
				String[] delimiters = field.getDelimiters();
				String delimStr = null;
				if ((delimiters != null) && (delimiters.length > 0)) {
					delimStr = delimiters[0];
				}
				properties.put(DataRecordMetadataXMLReaderWriter.DELIMITER_ATTR, delimStr);
				properties.put(DataRecordMetadataXMLReaderWriter.SIZE_ATTR, null);
			} else {
				properties.put(DataRecordMetadataXMLReaderWriter.DELIMITER_ATTR, null);
				properties.put(DataRecordMetadataXMLReaderWriter.SIZE_ATTR, String.valueOf(field.getSize()));
			}
			properties.put(DataRecordMetadataXMLReaderWriter.NULLABLE_ATTR, Boolean.toString(field.isNullable()));
			properties.put(DataRecordMetadataXMLReaderWriter.DEFAULT_ATTR, field.getDefaultValueStr());
			properties.put(DataRecordMetadataXMLReaderWriter.TRIM_ATTR, Boolean.toString(field.isTrim()));
			properties.put(DataRecordMetadataXMLReaderWriter.NULL_VALUE_ATTR, field.getNullValue());
			properties.put(DataRecordMetadataXMLReaderWriter.FORMAT_ATTR, field.getFormatStr());
			properties.put(DataRecordMetadataXMLReaderWriter.LOCALE_ATTR, MiscUtils.localeToString(MiscUtils.createLocale(field.getLocaleStr()))); // CLO-6293
			properties.put(DataRecordMetadataXMLReaderWriter.TIMEZONE_ATTR, new TimeZoneProvider(field.getTimeZoneStr()).toString()); // CLO-6293
			properties.put(DataRecordMetadataXMLReaderWriter.DESCRIPTION_ATTR, field.getDescription());
			
			// add custom properties, this includes length and scale for decimal type
			TypedProperties p = field.getFieldProperties();
			for (String s: p.stringPropertyNames()) {
				properties.put(s, p.getStringProperty(s));
			}
			
			properties = Collections.unmodifiableMap(new LinkedHashMap<String, String>(properties));
			
			// Store the value in the cache for next time.
			fieldPropertyCache.put(key, properties);
			
			return properties;
		}		
	}
	
	public static String toString(Object o) {
		if (o == null) {
			return null;
		}
		String s = o.toString();
		return !s.isEmpty() ? s : null;
	}
}
