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
import java.util.RandomAccess;

import org.jetel.ctl.TLUtils;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.primitive.IdentityArrayList;

/**
 * This data field implementation represents a list of fields, which are uniformly typed by a simple type
 * (string, integer, decimal, ...). Lists of lists (or maps) are not supported. Metadata for this data container
 * are same as for other data fields, only {@link DataFieldMetadata#getContainerType()} method returns
 * {@link DataFieldContainerType#LIST}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Jan 2012
 */
public class ListDataField extends DataField implements Iterable<DataField> {

	private static final long serialVersionUID = -3584218178444143371L;

	//representation of nested fields
	private List<DataField> fields;
	
	//size of internal representation of nested fields is managed manually,
	//since the tail of the list is used also for cached data fields (which are placed in the end)
	//for example if a field is removed, the field is actually only moved
	//to the end of the list and size is decremented
	private int size;
	
	//this common attribute of all datafield is actually ignored by list data field
	//and transparently delegated to the nested fields
	private boolean plain;
	
	//this cached list is returned by #getValue() method
	private ListDataFieldView<?> listView;
	
	// metadata used when creating inner DataFields
	private DataFieldMetadata singleValueMetadata;
	
	public ListDataField(DataFieldMetadata fieldMetadata) {
		this(fieldMetadata, false);
	}
	
	public ListDataField(DataFieldMetadata fieldMetadata, boolean plain) {
		super(fieldMetadata);
		if (fieldMetadata.getContainerType() != DataFieldContainerType.LIST) {
			throw new IllegalStateException("Unexpected operation, ListDataField can be created only for list fields.");
		}
		this.singleValueMetadata = fieldMetadata.duplicate();
		singleValueMetadata.setContainerType(DataFieldContainerType.SINGLE);

		fields = new IdentityArrayList<DataField>();
		size = 0;
		this.plain = plain;
		listView = new ListDataFieldView<Object>(this);
		
		//just for sure - this is not common to reset the field in other types of fields
		//but for the list field it seems to be better to reset it already here explicitly
		reset();
	}
	
	/**
	 * @return number of nested fields, 0 for null list
	 */
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
		setNull(metadata.isNullable());
	}
	
	/**
	 * @return add a reseted/empty field to the end of the list
	 */
	public DataField addField() {
		if (isNull) {
			setNull(false);
		}
		DataField result;
		if (size == fields.size()) {
			result = createDataField();
			fields.add(result);
		} else {
			result = fields.get(size);
		}
		result.reset();
		size++;
		return result;
	}
	
	/**
	 * Insert new field to list to the given index.
	 * @param index index of added field
	 * @return new inserted field
	 */
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
	 * Remove the given field from the list.
	 * Removed field is stored in a cache and can be used later by this {@link ListDataField} for {@link #addField()}
	 * operation, so the return value is still under control of this list.
	 * @param field the field which is requested be deleted
	 * @return true if field was removed, false otherwise
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
	 * Remove a field on the given index.
	 * Removed field is stored in a cache and can be used later by this {@link ListDataField} for {@link #addField()}
	 * operation, so the return value is still under control of this list.
	 * @param index index of a field, which is requested to be removed 
	 * @return removed field
	 * @throws IndexOutOfBoundsException if the index is out of range (index < 0 || index >= size())
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
	
	/**
	 * @param index index of requested field
	 * @return the requested field with the given index
	 * @throws IndexOutOfBoundsException if the index is out of range (index < 0 || index >= size())
	 */
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
	
	/**
	 * Truncate the list to zero size.
	 */
	public void clear() {
		size = 0;
	}
	
	@Override
	public ListDataField duplicate() {
	    ListDataField newListDataField = new ListDataField(metadata, plain);
	    newListDataField.setNull(isNull);
	    for (DataField field : this) {
	    	newListDataField.fields.add(field.duplicate());
	    }
	    newListDataField.size = size;
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

	/**
	 * Sets the give values to the list. All current values are removed.
	 * @param values list of values
	 */
	public void setValue(List<?> values) {
		if (values == null) {
			setNull(true);
		} else {
			clear();
			setNull(false);
		
			for (Object value : values) {
				addField().setValue(value);
			}
		}
	}

	@Override
	public void setValue(DataField fromField) {
		if (fromField == null || fromField.isNull()) {
			setNull(true);
		} else {
			if (fromField instanceof ListDataField) {
				setNull(false);
				clear();
				
				ListDataField fromListDataField = (ListDataField) fromField;
				for (DataField field : fromListDataField) {
					addField().setValue(field.getValue());
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
	 * This method returns list of values represented by the list of fields.
	 * The resulted list is a thin view to the real underlying values.
	 * All operations above the returned list are transparently applied 
	 * to this ListDataField.
	 * For example if a {@link List#add(Object)} is invoked, new data field
	 * is created and the given value is passed to the new data field.
	 * @see #getValueDuplicate()
	 */
	@Override
	public List<?> getValue() {
		if (isNull) {
			return null;
		} else {
			return listView;
		}
	}
	
	/**
	 * This method is alternative for untyped method {@link #getValue()}
	 * @param clazz
	 * @return 
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getValue(Class<T> clazz) {
		if (metadata.getDataType().getInternalValueClass() != clazz) {
			throw new ClassCastException("Class " + metadata.getDataType().getClass().getName() + " cannot be cast to " + clazz.getName());
		}
		return (List<T>) getValue();
	}

	@Override
	public List<?> getValueDuplicate() {
		if (isNull) {
			return null;
		}
		
		List<Object> result = new ArrayList<Object>();
		for (DataField field : this) {
			result.add(field.getValueDuplicate());
		}
		return result;
	}

	/**
	 * This method is alternative for untyped method {@link #getValueDuplicate()}
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getValueDuplicate(Class<T> clazz) {
		if (metadata.getDataType().getInternalValueClass() != clazz) {
			throw new ClassCastException("Class " + metadata.getDataType().getClass().getName() + " cannot be cast to " + clazz.getName());
		}
		return (List<T>) getValueDuplicate();
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

		Iterator<DataField> i = iterator();
		if (!i.hasNext())
			return "[]";

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (;;) {
			DataField e = i.next();
			if (e.isNull()) {
				sb.append("null");
			} else {
				sb.append(e.toString());
			}
			if (!i.hasNext()) {
				return sb.append(']').toString();
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
			ByteBufferUtils.encodeLength(buffer, isNull ? 0 : size + 1);

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
		clear();

		if (length == -1) {
			setNull(true);
		} else {
			for (int i = 0; i < length; i++) {
				addField().deserialize(buffer);
			}
			setNull(false);
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
	public int hashCode() {
		if (isNull) {
			return 0;
		}
		int hash = 1;
		for (int i = 0; i < size; ++i) {
			DataField field = fields.get(i);
			hash = 31 * hash + (field == null ? 0 : field.hashCode());
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object otherField) {
	    if (isNull || otherField == null) return false;
		if (this == otherField) return true;
	    
        if (otherField instanceof ListDataField) {
        	ListDataField otherListDataField = (ListDataField) otherField;
        	if (otherListDataField.isNull()) {
        		return false;
        	}
            if (!TLUtils.equals(metadata, otherListDataField.getMetadata())) {
                return false;
            }
            //size of both lists has to be same
            if (size != otherListDataField.getSize()) {
            	return false;
            }
            // check field by field that they are the same
            for (int i = 0; i < size; i++) {
            	final DataField subfield = fields.get(i);
            	final DataField otherSubfield = otherListDataField.getField(i);
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
	public int compareTo(Object otherObject) {
		if (isNull) return -1;
		if (otherObject == null) return 1;
	    if (this == otherObject) return 0;
	    
	    if (otherObject instanceof ListDataField) {
        	ListDataField otherListDataField = (ListDataField) otherObject; 
	    	
            if (!TLUtils.equals(metadata, otherListDataField.getMetadata())) {
                throw new RuntimeException("Can't compare - data field lists have different metadata objects assigned!");
            }
            
            if (otherListDataField.isNull()) {
            	return 1;
            }
            
            int otherListDataFieldSize = otherListDataField.getSize();
            int compareLength = Math.min(size, otherListDataFieldSize);
            
            int cmp;
            // check field by field that they are the same
            for (int i = 0; i < compareLength; i++) {
            	final DataField field = fields.get(i);
            	final DataField otherField = otherListDataField.getField(i);
            	if (!(field.isNull() && otherField.isNull())) {
                    cmp = fields.get(i).compareTo(otherListDataField.getField(i));
                    if (cmp != 0) {
                        return cmp;
                    }
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
            throw new ClassCastException("Can't compare ListDataField with " + otherObject.getClass().getName());
        }
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

		@Override
		public boolean hasNext() {
			return cursor != size;
		}

		@Override
		public DataField next() {
			if (cursor == size) {
				throw new NoSuchElementException();
			}
			DataField next = fields.get(cursor);
			cursor++;
			return next;
		}

		@Override
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
	private static class ListDataFieldView<T> extends AbstractList<T> implements RandomAccess {

		private ListDataField backedListDataField;
		
		public ListDataFieldView(ListDataField backedListDataField) {
			this.backedListDataField = backedListDataField;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T get(int index) {
			if (index < 0 || index >= backedListDataField.size) {
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);
			}
			
			return (T) backedListDataField.getField(index).getValueDuplicate();
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
		
		@SuppressWarnings("unchecked")
		@Override
		public T set(int index, T element) {
			if (index < 0 || index >= backedListDataField.size) {
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);
			}

			DataField field = backedListDataField.fields.get(index);
			T oldValue = (T) field.getValueDuplicate();
			field.setValue(element);
			return oldValue;
		}
		
		@Override
		public boolean add(T e) {
			backedListDataField.addField().setValue(e);
			return true;
		}
		
		@Override
		public void add(int index, T element) {
			DataField newField = backedListDataField.addField(index);
			newField.setValue(element);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public T remove(int index) {
			DataField removedField = backedListDataField.removeField(index);
			return (T) removedField.getValueDuplicate();
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
		public boolean addAll(Collection<? extends T> c) {
			Iterator<? extends T> it = c.iterator();
			while (it.hasNext()) {
				backedListDataField.addField().setValue(it.next());
			}
			return c.size() != 0;
		}

		@Override
		public boolean addAll(int index, Collection<? extends T> c) {
			if (index > backedListDataField.size || index < 0)
			    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + backedListDataField.size);

			//it is not optimal implementation - but I don't want to extend ListDataField interface more than necessary
			Iterator<? extends T> it = c.iterator();
			while (it.hasNext()) {
				backedListDataField.addField(index++).setValue(it.next());
			}
			return c.size() != 0;
		}
	}
	
}
