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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.jetel.ctl.TLUtils;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This data field implementation represents a map of fields, which are uniformly typed by a simple type
 * (string, integer, decimal, ...). Type of key is {@link String}. Maps of maps (or lists) are not supported. Metadata for this data container
 * are same as for other data fields, only {@link DataFieldMetadata#getContainerType()} method returns
 * {@link DataFieldContainerType#MAP}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31 Jan 2012
 */
public class MapDataField extends DataField {

	private static final long serialVersionUID = 7934647976668962456L;

	//representation of nested fields
	private Map<String, DataField> fields;
	
	//cache for unused but already allocated data fields
	private List<DataField> fieldsCache;
	
	//this common attribute of all datafield is actually ignored by map data field
	//and transparently delegated to the nested fields
	private boolean plain;
	
	//this cached map is returned by #getValue() method
	//it is simple view to underlying data object
	private MapDataFieldView<?> mapView;
	
	// metadata used when creating inner DataFields
	private DataFieldMetadata singleValueMetadata;
	
	public MapDataField(DataFieldMetadata fieldMetadata) {
		this(fieldMetadata, false);
	}
	
	public MapDataField(DataFieldMetadata fieldMetadata, boolean plain) {
		super(fieldMetadata);
		if (fieldMetadata.getContainerType() != DataFieldContainerType.MAP) {
			throw new IllegalStateException("Unexpected operation, MapDataField can be created only for map fields.");
		}
		this.singleValueMetadata = fieldMetadata.duplicate();
		singleValueMetadata.setContainerType(DataFieldContainerType.SINGLE);

		fields = new LinkedHashMap<String, DataField>();
		fieldsCache = new ArrayList<DataField>();
		this.plain = plain;
		mapView = new MapDataFieldView<Object>();
		
		//just for sure - this is not common to reset the field in other types of fields
		//but for the map field it seems to be better to reset it already here explicitly
		reset();
	}
	
	/**
	 * @return number of map entries, 0 for null map or empty map
	 */
	public int getSize() {
		return fields.size();
	}
	
	@Override
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			clear();
		}
	}
	
	@Override
	public void setToDefaultValue() {
		clear();
		setNull(metadata.isNullable());
	}
	
	/**
	 * Checks if the map has a data field associated with the given key.
	 */
	public boolean containsField(String fieldKey) {
		return fields.containsKey(fieldKey);
	}
	
	/**
	 * @return add a reseted/empty field to the map associated with the given field
	 */
	public DataField putField(String fieldKey) {
		if (isNull) {
			setNull(false);
		}
		DataField result;
		if (fieldsCache.isEmpty()) {
			result = createDataField();
		} else {
			result = fieldsCache.remove(fieldsCache.size() - 1);
		}
		DataField oldField = fields.put(fieldKey, result);
		if (oldField != null) {
			fieldsCache.add(oldField);
		}
		result.reset();
		return result;
	}
	
	/**
	 * Remove a data field associated with the given key.
	 * Removed field is stored in a cache and can be used later by this {@link MapDataField} for {@link #putField(String)}
	 * operation, so the return value is still under control of this map.
	 * @param fieldKey key of removed data field 
	 * @return removed field
	 */
	public DataField removeField(String fieldKey) {
		DataField removedField = fields.remove(fieldKey);
		if (removedField != null) {
			fieldsCache.add(removedField); //put the removed field back to the cache 
			return removedField;
		} else {
			return null;
		}
	}
	
	private DataField createDataField() {
		return DataFieldFactory.createDataField(singleValueMetadata, plain);
	}
	
	/**
	 * @param fieldKey key of requested data field
	 * @return the requested field associated with the given key or null if not data field found
	 */
	public DataField getField(String fieldKey) {
		return fields.get(fieldKey);
	}
	
	/**
	 * Truncate the map to zero size.
	 */
	public void clear() {
		fieldsCache.addAll(fields.values());
		fields.clear();
	}
	
	@Override
	public MapDataField duplicate() {
	    MapDataField newMapDataField = new MapDataField(metadata, plain);
	    newMapDataField.setNull(isNull);
	    for (Entry<String, DataField> fieldEntry : fields.entrySet()) {
	    	newMapDataField.fields.put(fieldEntry.getKey(), fieldEntry.getValue().duplicate());
	    }
	    return newMapDataField;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setValue(Object value) {
        if (value == null || value instanceof Map<?, ?>) {
            setValue((Map<String, ?>) value);
        } else {
        	BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + value.toString(), value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
        }
	}

	/**
	 * Sets the give values to this map. All current values are removed.
	 * @param values list of values
	 */
	public void setValue(Map<String, ?> values) {
		if (values == null) {
			setNull(true);
		} else {
			clear();
			setNull(false);
			
			for (Entry<String, ?> entry : values.entrySet()) {
				putField(entry.getKey()).setValue(entry.getValue());
			}
		}
	}

	@Override
	public void setValue(DataField fromField) {
		if (fromField == null || fromField.isNull()) {
			setNull(true);
		} else {
			if (fromField instanceof MapDataField) {
				setNull(false);
				clear();
				
				MapDataField fromMapDataField = (MapDataField) fromField;
				for (Entry<String, DataField> fieldEntry : fromMapDataField.fields.entrySet()) {
					putField(fieldEntry.getKey()).setValue(fieldEntry.getValue());
				}
			} else {
	            super.setValue(fromField);   
			}
		}
	}
	
	@Override
	public void reset() {
		if (metadata.isNullable()) {
			setNull(true);
		} else {
			setToDefaultValue();
		}
	}

	/**
	 * This method returns map of values represented by the map of fields.
	 * The resulted map is a thin view to the real underlying values.
	 * All operations above the returned map are transparently applied 
	 * to this MapDataField.
	 * For example if a {@link Map#put(Object)} is invoked, new data field
	 * is created and the given value is passed to the new data field.
	 * @see #getValueDuplicate()
	 */
	@Override
	public Map<String, ?> getValue() {
		if (isNull) {
			return null;
		} else {
			return mapView;
		}
	}
	
	/**
	 * This method is alternative for untyped method {@link #getValue()}
	 * @param clazz
	 * @return 
	 */
	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getValue(Class<T> clazz) {
		if (metadata.getDataType().getInternalValueClass() != clazz) {
			throw new ClassCastException("Class " + metadata.getDataType().getClass().getName() + " cannot be cast to " + clazz.getName());
		}
		return (Map<String, T>) getValue();
	}

	@Override
	public Map<String, ?> getValueDuplicate() {
		if (isNull) {
			return null;
		}
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		for (Entry<String, DataField> fieldEntry : fields.entrySet()) {
			result.put(fieldEntry.getKey(), fieldEntry.getValue().getValueDuplicate());
		}
		return result;
	}

	/**
	 * This method is alternative for untyped method {@link #getValueDuplicate()}
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getValueDuplicate(Class<T> clazz) {
		if (metadata.getDataType().getInternalValueClass() != clazz) {
			throw new ClassCastException("Class " + metadata.getDataType().getClass().getName() + " cannot be cast to " + clazz.getName());
		}
		return (Map<String, T>) getValueDuplicate();
	}

	@Override
	@Deprecated
	public char getType() {
		return metadata.getType();
	}

	public boolean isPlain() {
		return plain;
	}
	
	@Override
	public String toString() {
		if (isNull) {
			return metadata.getNullValue();
		}

		Iterator<Entry<String, DataField>> i = fields.entrySet().iterator();
		if (!i.hasNext())
			return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			Entry<String, DataField> e = i.next();
			String key = e.getKey();
			DataField value = e.getValue();
			sb.append(key);
			sb.append('=');
			if (value.isNull()) {
				sb.append("null");
			} else {
				sb.append(value.toString());
			}
			if (!i.hasNext()) {
				return sb.append('}').toString();
			}
			sb.append(", ");
		}
	}

	@Override
	public void fromString(CharSequence seq) {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be deserialized from string. List and map container types are not supported.");
	}

	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be deserialized from bytes. List and map container types are not supported.");
	}
	
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be deserialized from bytes. List and map container types are not supported.");
	}
	
	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be serialized to bytes. List and map container types are not supported.");
	}
	
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be serialized to bytes. List and map container types are not supported.");
	}
	
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			// encode null as zero, increment size of non-null values by one
			ByteBufferUtils.encodeLength(buffer, isNull ? 0 : fields.size() + 1);

			for (Entry<String, DataField> fieldEntry : fields.entrySet()) {
				ByteBufferUtils.encodeString(buffer, fieldEntry.getKey());
				fieldEntry.getValue().serialize(buffer);
			}
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}

	@Override
	public void deserialize(CloverBuffer buffer) {
		// encoded length is incremented by one, decrement it back to normal
		final int length = ByteBufferUtils.decodeLength(buffer) - 1;

		// clear the list
		clear();

		if (length == -1) {
			setNull(true);
		} else {
			for (int i = 0; i < length; i++) {
				putField(ByteBufferUtils.decodeString(buffer)).deserialize(buffer);
			}
			setNull(false);
		}
	}

	@Override
	public int getSizeSerialized() {
		if (isNull) {
			return 1;
		}
	    
		int length = ByteBufferUtils.lengthEncoded(fields.size() + 1);
	    
	    for (Entry<String, DataField> entry : fields.entrySet()) {
	    	length += ByteBufferUtils.lengthEncoded(entry.getKey()) + entry.getValue().getSizeSerialized();
	    }
	    
		return length;
	}
	
	@Override
	public int hashCode() {
		if (isNull) {
			return 0;
		}
		return HashCodeUtil.getHash(fields);
	}
	
	@Override
	public boolean equals(Object otherField) {
	    if (isNull || otherField == null) return false;
		if (this == otherField) return true;
	    
        if (otherField instanceof MapDataField) {
        	MapDataField otherMapDataField = (MapDataField) otherField;
        	if (otherMapDataField.isNull()) {
        		return false;
        	}
            if (!TLUtils.equals(metadata, otherMapDataField.getMetadata())) {
                return false;
            }
            int size = fields.size();
            //size of both maps has to be same
            if (size != otherMapDataField.getSize()) {
            	return false;
            }
            // check field by field that they are the same
            for (Entry<String, DataField> fieldEntry : fields.entrySet()) {
            	final String subfieldKey = fieldEntry.getKey();
            	final DataField subfield = fieldEntry.getValue();
            	final DataField otherSubfield = otherMapDataField.getField(subfieldKey);
            	if (!(subfield.isNull() && otherSubfield.isNull())) {
                    if (!subfield.equals(otherSubfield)) {
                        return false;
                    }
            	}
            }
            return true;
        } else {
            return false;
        }
	}

	@Override
	public int compareTo(Object otherField) {
		throw new UnsupportedOperationException(getMetadata().toString() + " cannot be compared to each other. Map fields are not supported.");
	}
	
	/**
	 * 
	 * @author Kokon (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 19 Jan 2012
	 */
	private final class MapDataFieldView<T> extends AbstractMap<String, T> implements Map<String, T> {

	    private transient Set<Map.Entry<String, T>> entrySet = null;
	    private transient volatile Set<String> keySet = null;

		@Override
		public int size() {
			return MapDataField.this.getSize();
		}

	    @SuppressWarnings("unchecked")
		@Override
		public T get(Object key) {
	    	DataField field = MapDataField.this.getField((String) key);
	    	return field != null ? (T) field.getValueDuplicate() : null;
	    }

		@Override
	    public boolean containsKey(Object key) {
	        return MapDataField.this.containsField((String) key);
	    }

		@SuppressWarnings("unchecked")
		@Override
	    public T put(String key, T value) {
			DataField field = MapDataField.this.getField(key);
			if (field != null) {
				T oldValue = (T) field.getValueDuplicate();
				field.setValue(value);
				return oldValue;
			} else {
				MapDataField.this.putField(key).setValue(value);
				return null;
			}
	    }

		@Override
	    public void putAll(Map<? extends String, ? extends T> m) {
	        for (Map.Entry<? extends String, ? extends T> entry : m.entrySet()) {
	        	MapDataField.this.putField(entry.getKey()).setValue(entry.getValue());
	        }
	    }

		@SuppressWarnings("unchecked")
		@Override
	    public T remove(Object key) {
			DataField oldField = MapDataField.this.removeField((String) key);
	        return oldField != null ? (T) oldField.getValueDuplicate() : null;
	    }

		@Override
	    public void clear() {
			MapDataField.this.clear();
	    }

		@Override
		public Set<String> keySet() {
	        Set<String> ks = keySet;
	        return (ks != null ? ks : (keySet = new KeySet()));
		}
		
	    private final class KeySet extends AbstractSet<String> {

	    	@Override
			public int size() {
				return MapDataFieldView.this.size();
			}

			@Override
			public boolean contains(Object o) {
				return MapDataFieldView.this.containsKey(o);
			}

			@Override
			public Iterator<String> iterator() {
				return new Iterator<String>() {
					private Iterator<Entry<String, T>> i = entrySet().iterator();

					@Override
					public boolean hasNext() {
						return i.hasNext();
					}

					@Override
					public String next() {
						return i.next().getKey();
					}

					@Override
					public void remove() {
						i.remove();
					}
				};
			}

			@Override
			public boolean remove(Object o) {
				if (MapDataFieldView.this.containsKey(o)) {
					MapDataFieldView.this.remove(o);
					return true;
				} else {
					return false;
				}
			}

			@Override
			public void clear() {
				MapDataFieldView.this.clear();
			}
	    }

		@Override
		public Set<Entry<String, T>> entrySet() {
	        Set<Map.Entry<String, T>> es = entrySet;
	        return es != null ? es : (entrySet = new EntrySet());
		}
		
	    private final class EntrySet extends AbstractSet<Map.Entry<String, T>> {

			@Override
			public int size() {
				return MapDataFieldView.this.size();
			}

			@SuppressWarnings("unchecked")
			@Override
			public boolean contains(Object o) {
	            if (!(o instanceof Map.Entry)) {
	                return false;
	            }
	            Map.Entry<String, T> e = (Map.Entry<String, T>) o;
	            return containsEntry(e);
			}

			private boolean containsEntry(Map.Entry<String, T> e) {
	            String key = e.getKey();
	            if (MapDataFieldView.this.containsKey(key)) {
	            	return MiscUtils.equals(MapDataFieldView.this.get(key), e.getValue());
	            }
	            return false;
			}
			
			@Override
			public Iterator<Entry<String, T>> iterator() {
				return new Iterator<Map.Entry<String, T>>() {
					final Iterator<Map.Entry<String, DataField>> parentIterator = MapDataField.this.fields.entrySet().iterator();
					@Override
					public boolean hasNext() {
						return parentIterator.hasNext();
					}
					@Override
					public Map.Entry<String, T> next() {
						final Entry<String, DataField> next = parentIterator.next();
						return new Map.Entry<String, T>() {
							@Override
							public final String getKey() {
								return next.getKey();
							}
							@SuppressWarnings("unchecked")
							@Override
							public final T getValue() {
								return (T) next.getValue().getValueDuplicate();
							}
							@SuppressWarnings("unchecked")
							@Override
							public final T setValue(T value) {
								Object oldValue = next.getValue().getValueDuplicate();
								next.getValue().setValue(value);
								return (T) oldValue;
							}
							@Override
					        public final boolean equals(Object o) {
					            if (!(o instanceof Map.Entry)) {
					                return false;
					            }
					            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
					            Object k1 = getKey();
					            Object k2 = e.getKey();
					            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
					                Object v1 = getValue();
					                Object v2 = e.getValue();
					                if (v1 == v2 || (v1 != null && v1.equals(v2))) {
					                    return true;
					                }
					            }
					            return false;
					        }
							@Override
					        public final int hashCode() {
								final String key = getKey();
								final T value = getValue();
					            return (key==null   ? 0 : key.hashCode()) ^
					                   (value==null ? 0 : value.hashCode());
					        }
							@Override
					        public final String toString() {
					            return getKey() + "=" + getValue();
					        }
						};
					}
					@Override
					public void remove() {
						parentIterator.remove();
					}
				};
			}

			@SuppressWarnings("unchecked")
			@Override
			public boolean remove(Object o) {
	            if (!(o instanceof Map.Entry)) {
	                return false;
	            }
	            Map.Entry<String, T> e = (Map.Entry<String, T>) o;
	            if (containsEntry(e)) {
		            MapDataFieldView.this.remove(e.getKey());
		            return true;
	            } else {
	            	return false;
	            }
			}

			@Override
			public void clear() {
				MapDataFieldView.this.clear();
			}
	    }
	}

}
