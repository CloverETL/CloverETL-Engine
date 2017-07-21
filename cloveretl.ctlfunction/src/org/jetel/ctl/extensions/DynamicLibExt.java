package org.jetel.ctl.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.Pair;
import org.jetel.util.string.UnicodeBlanks;

/**
 * Additional functions for dynamic field access.
 * 
 * @author Branislav Repcek (branislav.repcek@javlin.eu)
 */
public class DynamicLibExt extends TLFunctionLibraryExt {

	private static final String LIBRARY_NAME = "DynamicLib";

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
			throw new IllegalArgumentException("Input record cannot be null.");
		}
		
		if (fieldName == null) {
			throw new IllegalArgumentException("Field name cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		FieldPropertyCache cache = (FieldPropertyCache) context.getCache();
		
		return cache.getFieldProperties(recordMeta, fieldName);
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
			throw new IllegalArgumentException("Input record cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		if (fieldIndex < 0 || fieldIndex >= recordMeta.getFieldNamesArray().length) {
			throw new IndexOutOfBoundsException("Field index out of bounds.");
		}
		
		String fieldName = recordMeta.getFieldNamesArray()[fieldIndex];
		FieldPropertyCache cache = (FieldPropertyCache) context.getCache();
		
		return cache.getFieldProperties(recordMeta, fieldName);
	}
	
	@TLFunctionInitAnnotation
	public static void getFieldPropertiesInit(TLFunctionCallContext context) {
		if (context.getGraph() == null) {
			context.setCache(new FieldPropertyCache());
		} else {
			context.setCache(new FieldPropertyCache(context.getGraph()));
		}
	}
	
	/**
	 * Query value of field property of given field.
	 * 
	 * @param context function call context.
	 * @param record record to which the queried field belongs. Cannot be null.
	 * @param fieldName name of the field to query. Cannot be null. Note that field names are case sensitive.
	 * @param propertyName name of the property to query. Cannot be null or empty.
	 * 
	 * @return value of the field property converted to string. If property does not exist, null is returned.
	 */
	@TLFunctionAnnotation("Return value of given field property.")
	@CTL2FunctionDeclaration(impl = GetFieldPropertyValueFunction.class)
	public static String getFieldPropertyValue(TLFunctionCallContext context, DataRecord record, String fieldName, String propertyName) {
		
		if (UnicodeBlanks.isBlank(propertyName)) {
			throw new IllegalArgumentException("Property name cannot be blank.");
		}
		
		return getFieldProperties(context, record, fieldName).get(propertyName);
	}
	
	/**
	 * Query value of field property of given field.
	 * 
	 * @param context function call context.
	 * @param record record to which the queried field belongs. Cannot be null.
	 * @param fieldIndex index of the field to query. Index has to be greater than zero and less than number of fields
	 *        in the record. First field has index of zero.
	 * @param propertyName name of the property to query. Cannot be null or empty.
	 * 
	 * @return value of the field property converted to string. If property does not exist, null is returned.
	 */
	@TLFunctionAnnotation("Return value of given field property.")
	@CTL2FunctionDeclaration(impl = GetFieldPropertyValueFunction.class)
	public static String getFieldPropertyValue(TLFunctionCallContext context, DataRecord record, Integer fieldIndex, String propertyName) {
		
		if (UnicodeBlanks.isBlank(propertyName)) {
			throw new IllegalArgumentException("Property name cannot be blank.");
		}
		
		return getFieldProperties(context, record, fieldIndex).get(propertyName);
	}

	@TLFunctionInitAnnotation
	public static void getFieldPropertyValueInit(TLFunctionCallContext context) {
		if (context.getGraph() == null) {
			context.setCache(new FieldPropertyCache());
		} else {
			context.setCache(new FieldPropertyCache(context.getGraph()));
		}
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
			throw new IllegalArgumentException("Input record cannot be null.");
		}
		
		DataRecordMetadata recordMeta = record.getMetadata();
		RecordPropertyCache cache = (RecordPropertyCache) context.getCache();
		
		return cache.getRecordProperties(recordMeta);
	}

	@TLFunctionInitAnnotation
	public static void getRecordPropertiesInit(TLFunctionCallContext context) {
		if (context.getGraph() == null) {
			context.setCache(new RecordPropertyCache());
		} else {
			context.setCache(new RecordPropertyCache(context.getGraph()));
		}
	}
	
	/**
	 * Query value of a property of the given record.
	 * 
	 * @param context function call context.
	 * @param record record to query. Cannot be null.
	 * @param propertyName name of the property to query. Cannot be null or empty.
	 * 
	 * @return value of the record property converted to string. If property does not exist, null is returned.
	 */
	@TLFunctionAnnotation("Query the value of given record property.")
	@CTL2FunctionDeclaration(impl = GetRecordPropertyValueFunction.class)
	public static String getRecordPropertyValue(TLFunctionCallContext context, DataRecord record, String propertyName) {

		if (UnicodeBlanks.isBlank(propertyName)) {
			throw new IllegalArgumentException("Property name cannot be blank.");
		}
		
		return getRecordProperties(context, record).get(propertyName);
	}
	
	@TLFunctionInitAnnotation
	public static void getRecordPropertyValueInit(TLFunctionCallContext context) {
		if (context.getGraph() == null) {
			context.setCache(new RecordPropertyCache());
		} else {
			context.setCache(new RecordPropertyCache(context.getGraph()));
		}
	}
	
	/**
	 * Get the name of the metadata which defines the structure of the record.
	 * 
	 * @param context function call context.
	 * @param record record to query. Cannot be null.
	 * 
	 * @return name of the record's metadata.
	 */
	@TLFunctionAnnotation("Return name of the record's metadata.")
	@CTL2FunctionDeclaration(impl = GetRecordNameFunction.class)
	public static String getRecordName(TLFunctionCallContext context, DataRecord record) {
		
		if (record == null) {
			throw new IllegalArgumentException("Input record cannot be null.");
		}
		
		return record.getMetadata().getName();
	}
	
//	/**
//	 * Get list of names of all the key fields of given record.
//	 * 
//	 * @param context function call context.
//	 * @param record record to query. Cannot be null.
//	 * 
//	 * @return list of strings representing the names of the key fields of provided record.
//	 */
//	@TLFunctionAnnotation("Get list of all key fields of given record.")
//	@CTL2FunctionDeclaration(impl = GetRecordKeyFieldsFunction.class)
//	public static List< String > getRecordKeyFields(TLFunctionCallContext context, DataRecord record) {
//		
//		if (record == null) {
//			throw new IllegalArgumentException("Input record cannot be null.");
//		}
//
//		return record.getMetadata().getKeyFieldNames();
//	}
	
//	/**
//	 * Get offset of the field within fixed-length metadata. Offset is the number of characters from the beginning of the
//	 * record. Note that this function is only available for fixed-length record.
//	 * 
//	 * @param context function call context.
//	 * @param record parent record of the queried field. Cannot be null.
//	 * @param fieldName name of the field to query. Cannot be null.
//	 * 
//	 * @return number of characters indicating the position of the field in the record (its offset).
//	 *         Returns -1 if the field does not exist.
//	 */
//	@TLFunctionAnnotation("Get the offset of the field within fixed-length record.")
//	@CTL2FunctionDeclaration(impl = GetFieldOffsetFunction.class)
//	public static int getFieldOffset(TLFunctionCallContext context, DataRecord record, String fieldName) {
//		
//		if (record == null) {
//			throw new IllegalArgumentException("Input record cannot be null.");
//		}
//		
//		if (fieldName == null) {
//			throw new IllegalArgumentException("Field name cannot be null.");
//		}
//		
//		if (record.getMetadata().getParsingType() != DataRecordParsingType.FIXEDLEN) {
//			throw new IllegalArgumentException("Field offset is only available for fixed-length record. Metadata '" + 
//					record.getMetadata().getName() + "' defines a " + 
//					record.getMetadata().getParsingType().toString().toLowerCase() + " record.");
//		}
//		
//		return record.getMetadata().getFieldOffset(fieldName);
//	}
//
//	/**
//	 * Get offset of the field within fixed-length metadata. Offset is the number of characters from the beginning of the
//	 * record. Note that this function is only available for fixed-length record.
//	 * 
//	 * @param context function call context.
//	 * @param record parent record of the queried field. Cannot be null.
//	 * @param fieldIndex index of the field to query. Index has to be greater than zero and less than number of fields
//	 *        in the record. First field has index of zero.
//	 * 
//	 * @return number of characters indicating the position of the field in the record (its offset).
//	 *         Returns -1 if the field does not exist.
//	 */
//	@TLFunctionAnnotation("Get the offset of the field within fixed-length record.")
//	@CTL2FunctionDeclaration(impl = GetFieldOffsetFunction.class)
//	public static int getFieldOffset(TLFunctionCallContext context, DataRecord record, Integer fieldIndex) {
//		
//		if (record == null) {
//			throw new IllegalArgumentException("Input record cannot be null.");
//		}
//		
//		DataRecordMetadata recordMeta = record.getMetadata();
//		if (fieldIndex < 0 || fieldIndex >= recordMeta.getFieldNamesArray().length) {
//			throw new IndexOutOfBoundsException("Field index out of bounds.");
//		}
//		
//		String fieldName = recordMeta.getFieldNamesArray()[fieldIndex];
//		
//		if (record.getMetadata().getParsingType() != DataRecordParsingType.FIXEDLEN) {
//			throw new IllegalArgumentException("Field offset is only available for fixed-length record. Metadata '" + 
//					record.getMetadata().getName() + "' defines a " +
//					record.getMetadata().getParsingType().toString().toLowerCase() + " record.");
//		}
//		
//		return record.getMetadata().getFieldOffset(fieldName);
//	}
	
	/**
	 * Query the list of names of all the fields in the record.
	 * 
	 * @param context function call context.
	 * @param record record to query. Cannot be null.
	 * 
	 * @return list of field names of all fields in the given record. Fields are returned in the order in which they
	 *         are declared.
	 */
	@TLFunctionAnnotation("Query the list of names of all the fields in the record.")
	@CTL2FunctionDeclaration(impl = GetFieldNamesFunction.class)
	public static List< String > getFieldNames(TLFunctionCallContext context, DataRecord record) {

		if (record == null) {
			throw new IllegalArgumentException("Input record cannot be null.");
		}
		
		String[] fieldNames = record.getMetadata().getFieldNamesArray();
		List< String > result = new ArrayList< String >(fieldNames.length);
		Collections.addAll(result, fieldNames);
		
		return result;
	}
	
	/**
	 * Get the data type of the metadata field.
	 * 
	 * @param context function call context.
	 * @param record record to query. Cannot be null.
	 * @param fieldName name of the field to query. Cannot be null and the field has to exist within the metadata,
	 * 
	 * @return type of the queried field in the metadata.
	 */
	@TLFunctionAnnotation("Query the data type of a metadata field.")
	@CTL2FunctionDeclaration(impl = GetFieldTypeFunction.class)
	public static String getFieldType(TLFunctionCallContext context, DataRecord record, String fieldName) {
		
		if (!record.hasField(fieldName)) {
			throw new JetelRuntimeException("Field '" + fieldName + "' is not defined in metadata '" + record.getMetadata().getName() + "'.");
		}
		return record.getField(fieldName).getMetadata().getDataType().getName();
	}
	
	public class GetFieldPropertiesFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: (fieldName | fieldIndex), record
			Object fieldId = stack.pop();
			DataRecord record = stack.popRecord();
			
			if (fieldId == null) {
				throw new IllegalArgumentException("Field index or field name cannot be null.");
			}
			
			if (fieldId instanceof String) {
				stack.push(getFieldProperties(context, record, (String) fieldId));
			} else if (fieldId instanceof Integer) {
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
	
	public class GetFieldPropertyValueFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: propertyName, (fieldName | fieldIndex), record
			String propertyName = stack.popString();
			Object fieldId = stack.pop();
			DataRecord record = stack.popRecord();
			
			if (fieldId == null) {
				throw new IllegalArgumentException("Field index or field name cannot be null.");
			}
			
			if (fieldId instanceof String) {
				stack.push(getFieldPropertyValue(context, record, (String) fieldId, propertyName));
			} else if (fieldId instanceof Integer) {
				stack.push(getFieldPropertyValue(context, record, (Integer) fieldId, propertyName));
			} else {
				throw new IllegalArgumentException("Unsupported parameter type: " + fieldId.getClass().getCanonicalName());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
			getFieldPropertyValueInit(context);
		}
	}

//	public class GetFieldOffsetFunction implements TLFunctionPrototype {
//
//		@Override
//		public void execute(Stack stack, TLFunctionCallContext context) {
//			// Stack layout: fieldName | fieldIndex, record
//			Object fieldId = stack.pop();
//			DataRecord record = stack.popRecord();
//			
//			if (fieldId == null) {
//				throw new IllegalArgumentException("Field index or field name cannot be null.");
//			}
//			
//			if (fieldId instanceof String) {
//				stack.push(getFieldOffset(context, record, (String) fieldId));
//			} else if (fieldId instanceof Integer) {
//				stack.push(getFieldOffset(context, record, (Integer) fieldId));
//			} else {
//				throw new IllegalArgumentException("Unsupported parameter type: " + fieldId.getClass().getCanonicalName());
//			}
//		}
//
//		@Override
//		public void init(TLFunctionCallContext context) {
//		}
//	}
	
	public class GetRecordPropertiesFunction implements TLFunctionPrototype {

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

	public class GetRecordPropertyValueFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: propertyName, record
			String propertyName = stack.popString();
			DataRecord record = stack.popRecord();
			stack.push(getRecordPropertyValue(context, record, propertyName));
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
			getRecordPropertyValueInit(context);
		}
	}

	public class GetRecordNameFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: record
			stack.push(getRecordName(context, stack.popRecord()));
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

//	public class GetRecordKeyFieldsFunction implements TLFunctionPrototype {
//
//		@Override
//		public void execute(Stack stack, TLFunctionCallContext context) {
//			// Stack layout: record
//			stack.push(getRecordKeyFields(context, stack.popRecord()));
//		}
//		
//		@Override
//		public void init(TLFunctionCallContext context) {
//		}
//	}

	public class GetFieldNamesFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: record
			stack.push(getFieldNames(context, stack.popRecord()));
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

	public class GetFieldTypeFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: fieldName, record
			String fieldName = stack.popString();
			DataRecord record = stack.popRecord();
			stack.push(getFieldType(context, record, fieldName));
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	/**
	 * <p>Cache which stores converted record properties so that multiple queries for the same property map do not result
	 * in complicated and costly conversion.</p>
	 * <p>Cache is initialized lazily - cache entries are added when they are queried for the first time.</p>
	 * 
	 * @author Branislav Repcek (branislav.repcek@javlin.eu)
	 */
	static class RecordPropertyCache extends TLCache {

		private TransformationGraph graph;

		private HashMap< DataRecordMetadata, Map< String, String > > recordPropertyCache = 
				new HashMap< DataRecordMetadata, Map< String, String > >();
	
		public RecordPropertyCache() {
		}

		public RecordPropertyCache(TransformationGraph graph) {
			this.graph = graph;
		}
		
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

			if (graph == null) {
				return new HashMap< String, String >();
			}

			Map< String, String > properties = recordPropertyCache.get(metadata);
			
			if (properties != null) {
				return properties;
			}
			
			properties = new HashMap< String, String >();
			
			Enumeration< ? > propertyNames = metadata.getRecordProperties().propertyNames();
			
			// Walk through all the properties and convert each value to string. nulls are preserved.
			while (propertyNames.hasMoreElements()) {
				String name = (String) propertyNames.nextElement();
				Object value = metadata.getRecordProperties().get(name);
				
				if (value != null) {
					properties.put(name, value.toString());
				} else {
					properties.put(name, null);
				}
			}
			
			// make the map read-only
			properties = Collections.unmodifiableMap(properties);

			// Store the value in the cache for next time.
			recordPropertyCache.put(metadata, properties);

			return properties;
		}
	}
	
	/**
	 * <p>Cache which stores converted field properties so that multiple queries for the same property map do not result
	 * in complicated and costly conversion.</p>
	 * <p>Cache is initialized lazily - cache entries are added when they are queried for the first time.</p>
	 * 
	 * @author Branislav Repcek (branislav.repcek@javlin.eu)
	 */
	static class FieldPropertyCache extends TLCache {
		
		private TransformationGraph graph;
		
		/**
		 * Cache for resolved and converted field properties. Key is pair<metadata, fieldName> while the value is
		 * map<fieldPropName, fieldPropValue>.
		 */
		private HashMap< Pair< DataRecordMetadata, String >, Map< String, String > > fieldPropertyCache = 
				new HashMap< Pair< DataRecordMetadata, String >, Map< String, String > >();
		
		public FieldPropertyCache() {
		}
		
		public FieldPropertyCache(TransformationGraph graph) {
			this.graph = graph;
		}
		
		/**
		 * Get field property map for given metadata field. If the field has been queried before, the value is returned
		 * from cache. Otherwise new cache entry is created and stored in the cache. The cache never expires.
		 * 
		 * @param metadata metadata which contains the queried field.
		 * @param fieldName name of the field to query. Cannot be null, case sensitive.
		 * 
		 * @return map of all properties of given field. If no properties are defined, returns empty map. Never returns null.
		 */
		public Map< String, String > getFieldProperties(DataRecordMetadata metadata, String fieldName) {
			
			if (graph == null) {
				return new HashMap< String, String >();
			}
			
			Pair< DataRecordMetadata, String > key = new Pair< DataRecordMetadata, String >(metadata, fieldName);
			Map< String, String > properties = fieldPropertyCache.get(key);

			if (properties != null) {
				return properties;
			}

			// Not in the cache, so build the cache entry
			properties = new HashMap< String, String >();
			
			DataFieldMetadata fieldMeta = metadata.getField(fieldName);
			
			if (fieldMeta == null) {
				throw new IllegalArgumentException("Field '" + fieldName + "' not found in metadata '" + metadata.getName() + "'.");
			}
			
			Enumeration< ? > propertyNames = fieldMeta.getFieldProperties().propertyNames();
			
			// Walk through all the properties and convert each value to string. nulls are preserved.
			while (propertyNames.hasMoreElements()) {
				String name = (String) propertyNames.nextElement();
				Object value = fieldMeta.getFieldProperties().get(name);
				
				if (value != null) {
					properties.put(name, value.toString());
				} else {
					properties.put(name, null);
				}
			}
			
			// make the map read-only
			properties = Collections.unmodifiableMap(properties);
			
			// Store the value in the cache for next time.
			fieldPropertyCache.put(key, properties);
			
			return properties;
		}		
	}
}
