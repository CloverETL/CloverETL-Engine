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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldCardinalityType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.StringUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Jan 2012
 */
public class ListDataField extends DataField implements Iterable<DataField> {

	private static final long serialVersionUID = -3584218178444143371L;

	private List<DataField> fields;
	
	private int size;
	
	private boolean plain;
	
	private ListDataFieldView listView;
	
	// metadata used when creating inner DataFields
	private DataFieldMetadata singleValueMetadata;
	
	public ListDataField(DataFieldMetadata fieldMetadata) {
		this(fieldMetadata, false);
	}
	
	public ListDataField(DataFieldMetadata fieldMetadata, boolean plain) {
		super(fieldMetadata);
		if (fieldMetadata.getCardinalityType() != DataFieldCardinalityType.LIST) {
			throw new IllegalStateException("Unexpected operation, ListDataField can be created only for list fields.");
		}
		this.singleValueMetadata = fieldMetadata.duplicate();
		singleValueMetadata.setCardinalityType(DataFieldCardinalityType.SINGLE);

		fields = new ArrayList<DataField>();
		size = 0;
		this.plain = plain;
		listView = new ListDataFieldView(this);
		
		//just for sure - this is not common to reset the field in other types of fields
		//but for the list field it seems to be better to reset it already here explicitly
		reset();
	}
	
	public int getSize() {
		return size;
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
		
		try {
            Object val;
            if ((val = metadata.getDefaultValue()) != null) {
                addField().setValue(val);
            } else 	if (metadata.getDefaultValueStr() != null) {
				DataField defaultField = addField();
				//do we really need to convert the string form of default value to 'SpecChar'?
				//this conversion was already done in DataRecordMetadataXMLReaderWriter.parseRecordMetadata()
				defaultField.fromString(StringUtils.stringToSpecChar(metadata.getDefaultValueStr()));
				metadata.setDefaultValue(defaultField.getValueDuplicate());
			} else if (metadata.isNullable()) {
				setNull(true);
			} else {
				setNull(false);
			}
		} catch (Exception ex) {
			// here, the only reason to fail is bad DefaultValue
			throw new BadDataFormatException(metadata.getName() + " has incorrect default value", metadata.getDefaultValueStr());
		}
	}
	
	public DataField addField() {
		if (isNull) {
			setNull(false);
		}
		if (size == fields.size()) {
			fields.add(createDataField());
		}
		DataField result = fields.get(size);
		result.reset();
		size++;
		return result;
	}
	
	public DataField addField(int index) {
		if (index < 0 || index > size) {
		    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}
		if (isNull) {
			setNull(false);
		}
		DataField newField;
		if (size == fields.size()) { //do we have some field in the cache
			newField = createDataField(); //no - so create a new one
		} else {
			newField = fields.get(size - 1); //yes - lets take the last one from the cache 
		}
		newField.reset();
		fields.add(index, newField);
		size++;
		return newField;
	}
	
	/**
	 * Removed field is stored in a cache and can be used later by this {@link ListDataField} for {@link #addField()}
	 * operation.
	 * @param field
	 * @return
	 */
	public boolean removeField(DataField field) {
		if (field == null || isNull) {
			return false;
		}
		int index = fields.indexOf(field);
		
		if (index >= 0 && index < size) {
			fields.remove(field);
			fields.add(field); //put the removed field back to the cache 
			size--;
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Removed field is stored in a cache and can be used later by this {@link ListDataField} for {@link #addField()}
	 * operation.
	 * @param index
	 * @return
	 */
	public DataField removeField(int index) {
		if (index < 0 || index >= size) {
		    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}
		
		DataField removedField = fields.remove(index);
		fields.add(removedField); //put the removed field back to the cache 
		size--;
		return removedField;
	}
	
	private DataField createDataField() {
		return DataFieldFactory.createDataField(singleValueMetadata, plain);
	}
	
	public DataField getField(int index) {
		if (index < 0 || index >= size) {
			if (isNull) {
				throw new IndexOutOfBoundsException(String.format("List data field is null. Field with index %d does not exist.", index));
			} else {
				throw new IndexOutOfBoundsException(String.format("Field with index %d does not exist.", index));
			}
		}
		return fields.get(index);
	}
	
	public void clear() {
		size = 0;
	}
	
	@Override
	public DataField duplicate() {
	    ListDataField newListDataField = new ListDataField(metadata, plain);
	    newListDataField.setNull(isNull);
	    for (DataField field : this) {
	    	newListDataField.fields.add(field.duplicate());
	    }
	    return newListDataField;
	}

	@Override
	public void setValue(Object value) {
        if (value == null || value instanceof List<?>) {
            setValue((List<?>) value);
        } else {
        	BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + value.toString(), value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
        }
	}

	public void setValue(List<?> values) {
		if (values == null) {
			setNull(true);
		} else {
			clear();
		
			for (Object value : values) {
				addField().setValue(value);
			}
			setNull(false);
		}
	}

	@Override
	public void setValue(DataField fromField) {
		if (fromField == null || fromField.isNull()) {
			setNull(true);
		}
		
		if (fromField instanceof ListDataField) {
			ListDataField fromListDataField = (ListDataField) fromField;
			for (DataField field : fromListDataField) {
				addField().setValue(field.getValue());
			}
		} else {
			//lets try this way - last chance - maybe the field's value is a list
            setValue(fromField.getValue());   
		}
	}
	
	@Override
	public void reset() {
		if (metadata.isNullable()) {
			setNull(true);
		} else if (metadata.isDefaultValueSet()) {
			setToDefaultValue();
		} else {
			clear();
		}
	}

	@Override
	public List<Object> getValue() {
		if (isNull) {
			return null;
		} else {
			return listView;
		}
	}

	@Override
	public Object getValueDuplicate() {
		if (isNull) {
			return null;
		}
		
		List<Object> result = new ArrayList<Object>();
		for (DataField field : this) {
			result.add(field.getValueDuplicate());
		}
		return result;
	}

	@Override
	@Deprecated
	public char getType() {
		return DataFieldType.LIST.getObsoleteIdentifier();
	}

	public boolean isPlain() {
		return plain;
	}
	
	@Override
	public String toString() {
		if (isNull) {
			return "ListDataField is null";
		}

		int i = 1;
		StringBuilder sb = new StringBuilder();
		sb.append("ListDataField size=" + size);
		for (DataField field : this) {
			sb.append(field.toString());
			sb.append('#');
			sb.append(i++);
			sb.append('=');
			sb.append(field.toString());
			sb.append('\n');
		}
		return sb.toString();
	}

	@Override
	public void fromString(CharSequence seq) {
		throw new UnsupportedOperationException("ListDataField cannot be deserialized from string.");
	}

	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		throw new UnsupportedOperationException("ListDataField cannot be deserialized from byte buffer.");
	}
	
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		throw new UnsupportedOperationException("ListDataField cannot be deserialized from clover buffer.");
	}
	
	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		throw new UnsupportedOperationException("ListDataField cannot be serialized to byte buffer.");
	}
	
	@Override
	public void toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		throw new UnsupportedOperationException("ListDataField cannot be serialized to clover buffer.");
	}
	
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			// encode null as zero, increment size of non-null values by one
			ByteBufferUtils.encodeLength(buffer, isNull ? 0 : size + 1);

			//is bulk operation worth enough?
			for (DataField field : this) {
				field.serialize(buffer);
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
		reset();

		if (length == 0) {
			setNull(true);
		} else {
			for (int i = 0; i < length - 1; i++) {
				addField().deserialize(buffer);
			}
			setNull(false);
		}
	}

	@Override
	public boolean equalsValue(Object otherField) {
		if (this == otherField) return true;
	    
        if (otherField instanceof ListDataField) {
        	ListDataField otherListDataField = (ListDataField) otherField; 
            if (metadata != otherListDataField.getMetadata()) {
                return false;
            }
            if (isNull || otherListDataField.isNull()) {
            	return isNull == otherListDataField.isNull();
            }
            
            //size of both lists has to be same
            if (size != otherListDataField.getSize()) {
            	return false;
            }
            // check field by field that they are the same
            for (int i = 0; i < size; i++) {
            	final DataField subfield = fields.get(i);
            	final DataField otherSubfield = otherListDataField.getField(i);
                if (!subfield.equals(otherSubfield)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
	}

	@Override
	public int compareTo(Object otherField) {
	    if (this == otherField) return 0;
	    
	    if (otherField instanceof ListDataField) {
        	ListDataField otherListDataField = (ListDataField) otherField; 
	    	
            if (metadata != otherListDataField.getMetadata()) {
                throw new RuntimeException("Can't compare - data field lists have different metadata objects assigned!");
            }
            
            if (isNull) {
            	return otherListDataField.isNull() ? 0 : -1;
            }
            if (otherListDataField.isNull()) {
            	return 1;
            }
            
            int otherListDataFieldSize = otherListDataField.getSize();
            int compareLength = Math.min(size, otherListDataFieldSize);
            
            int cmp;
            // check field by field that they are the same
            for (int i = 0; i < compareLength; i++) {
                cmp = fields.get(i).compareTo(otherListDataField.getField(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            if (size == otherListDataFieldSize) {
            	return 0;
            } else if (size > otherListDataFieldSize) {
            	return 1;
            } else {
            	return -1;
            }
        } else {
            throw new ClassCastException("Can't compare ListDataField with " + otherField.getClass().getName());
        }
	}

	@Override
	public int getSizeSerialized() {
		if (isNull) {
			return 1;
		}
	    
		int length = ByteBufferUtils.lengthEncoded(size + 1);
	    
	    for (DataField field : this) {
	    	length += field.getSizeSerialized();
	    }
	    
		return length;
	}

	@Override
	public Iterator<DataField> iterator() {
		return new Itr();
	}

	private class Itr implements Iterator<DataField> {
		/**
		 * Index of element to be returned by subsequent call to next.
		 */
		int cursor = 0;

		public boolean hasNext() {
			return cursor != size;
		}

		public DataField next() {
			if (cursor == size) {
				throw new NoSuchElementException();
			}
			DataField next = fields.get(cursor);
			cursor++;
			return next;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * 
	 * NOTE: does not count with backedListDataField.isNull == true, in that case empty list is considered
	 * @author Kokon (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 19 Jan 2012
	 */
	private static class ListDataFieldView extends AbstractList<Object> {

		private ListDataField backedListDataField;
		
		/**
		 * 
		 */
		public ListDataFieldView(ListDataField backedListDataField) {
			this.backedListDataField = backedListDataField;
		}

		@Override
		public Object get(int index) {
			if (index < 0 || index >= backedListDataField.size) {
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);
			}
			
			return backedListDataField.getField(index).getValue();
		}

		@Override
		public int size() {
			return backedListDataField.size;
		}
		
		@Override
		public boolean isEmpty() {
			return backedListDataField.size == 0;
		}

		@Override
		public boolean contains(Object o) {
			return indexOf(o) >= 0;
		}
		
		@Override
		public int indexOf(Object o) {
			if (o == null) {
			    for (int i = 0; i < backedListDataField.size; i++) {
					if (backedListDataField.fields.get(i).getValue() == null) {
					    return i;
					}
			    }
			} else {
			    for (int i = 0; i < backedListDataField.size; i++) {
					if (o.equals(backedListDataField.fields.get(i).getValue())) {
					    return i;
					}
			    }
			}
			return -1;
		}
		
		@Override
		public int lastIndexOf(Object o) {
			if (o == null) {
			    for (int i = backedListDataField.size - 1; i >= 0; i--) {
					if (backedListDataField.fields.get(i).getValue() == null) {
					    return i;
					}
			    }
			} else {
			    for (int i = backedListDataField.size - 1; i >= 0; i--) {
					if (o.equals(backedListDataField.fields.get(i).getValue())) {
					    return i;
					}
			    }
			}
			return -1;
		}
		
		@Override
		public Object set(int index, Object element) {
			if (index < 0 || index >= backedListDataField.size) {
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);
			}

			DataField field = backedListDataField.fields.get(index);
			Object oldValue = field.getValue();
			field.setValue(element);
			return oldValue;
		}
		
		@Override
		public boolean add(Object e) {
			backedListDataField.addField().setValue(e);
			return true;
		}
		
		@Override
		public void add(int index, Object element) {
			DataField newField = backedListDataField.addField(index);
			newField.setValue(element);
		}
		
		@Override
		public Object remove(int index) {
			DataField removedField = backedListDataField.removeField(index);
			return removedField.getValue();
		}

		@Override
		public boolean remove(Object o) {
			if (o == null) {
				for (int index = 0; index < backedListDataField.size; index++)
					if (backedListDataField.fields.get(index).getValue() == null) {
						backedListDataField.removeField(index);
						return true;
					}
			} else {
				for (int index = 0; index < backedListDataField.size; index++)
					if (o.equals(backedListDataField.fields.get(index).getValue())) {
						backedListDataField.removeField(index);
						return true;
					}
			}
			return false;
		}
		
		@Override
		public void clear() {
			backedListDataField.clear();
		}
		
		@Override
		public boolean addAll(Collection<? extends Object> c) {
			Iterator<? extends Object> it = c.iterator();
			while (it.hasNext()) {
				backedListDataField.addField().setValue(it.next());
			}
			return c.size() != 0;
		}

		@Override
		public boolean addAll(int index, Collection<? extends Object> c) {
			if (index > backedListDataField.size || index < 0)
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);

			//it is not optimal implementation - but I don't want to extend ListDataField interface more than necessary
			Iterator<? extends Object> it = c.iterator();
			while (it.hasNext()) {
				backedListDataField.addField(index++).setValue(it.next());
			}
			return c.size() != 0;
		}

	}
}
