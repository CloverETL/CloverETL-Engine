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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 *  An interface that represents data field and its value.
 *
 * @author      David Pavlis
 * @since       March 26, 2002
 * @see         OtherClasses
 */
public interface DataField extends Serializable, Comparable<Object> {

	/**
	 * Creates deep copy of existing field. 
	 * 
	 * @return new Field (exact copy of current field)
	 */
	public DataField duplicate();
	
	/**
	 * Sets current field's value from DataField passed as argument
	 * @param fieldFrom DataField from which to get the value
     * @deprecated use setValue(DataField) instead
	 */
	@Deprecated
	public void copyFrom(DataField fieldFrom);
	
	/**
	 *  An operation that sets value of the data field.
     *  The given object is never persisted in this data field.
     *  The method implementation should do deep copy of the given object everytime.
	 *
	 * @param  _value  The new Value value
	 * @since
	 */
	public void setValue(Object _value);
	
	/**
	 * An operation that sets value of the data field from another data field.
	 * 
	 * @param _value data field for getting value
	 */
	public void setValue(DataField fromField);

	/**
	 *  An operation that sets value of the data field to default value.
	 *  If default value is not preset, tries to set field value to null.
	 * @exception  BadDataFormatException  Description of the Exception
	 */
	public void setToDefaultValue();

	/**
	 *  Sets the Null value indicator/status. If passed-in value
	 * is True, then the field's internal value is set to NULL. The nullability
	 * of the field is checked and if the NULL value is not permitted, the
	 * BadDataFormatException is thrown.
	 *
	 * @param  isNull  The new Null value
	 * @since          September 16, 2002
	 */
	public void setNull(boolean isNull);

	/**
	 * An operation which sets/resets field to its
     * initial value (just after it was created by JVM) - 
     * it varies depending on type of field.<br>
     * Nullable fields are set to NULL, non-nullable are zeroed, if they
     * have default value defined, then default value is assigned.
     * 
	 */
	public void reset();
    
    
	/**
	 *  Returns the internal value of the data field. If field's value is
	 * deemed to be NULL (isNull() return true) then the returned
	 * value is NULL. Otherwise the internal value is converted
	 * to appropriate object representation and returned.<br>
	 *
	 * @return    The Value value
	 * @since
	 */
	public Object getValue();

    /**
     *  Returns the duplicate of the internal value of the data field. If field's value is
     * deemed to be NULL (isNull() return true) then the returned
     * value is NULL. Otherwise the duplicate of the internal value is converted
     * to appropriate object representation and returned.<br>
     *
     * @return    The Value value
     * @since
     */
    public Object getValueDuplicate();
    
	/**
	 *  Returns type of the field (String, Numeric, Date, etc.)
	 *
	 * @return    The Type value
	 * @since
	 * @deprecated use {@link #getMetadata()} and {@link DataFieldMetadata#getDataType()} instead
	 */
    @Deprecated
	public char getType();

	/**
	 *  Returns metadata associated with the data field
	 *
	 * @return    The Metadata value
	 * @since
	 */
	public DataFieldMetadata getMetadata();

	/**
	 *  Checks the field value for being NULL.
	 *
	 * @return    True if the data field's internal value is
	 * deemed to be NULL otherwise false;
	 * @since     September 16, 2002
	 */
	public boolean isNull();

	/**
	 *  Converts field's value into String representation
	 *
	 * @return    Description of the Returned Value
	 * @since
	 */
	@Override
	public String toString();

	/**
	 *  Parses field's value from string
	 *
	 * @param	seq	String representation of the value to be set  
	 * @since 11.12.2006
	 */
	public void fromString(CharSequence seq);

	/**
	 *  Get the field's value from ByteBuffer
	 *
	 * @param  decoder                       Charset decoder which could be used to decode characters
	 * @param  dataBuffer                    Description of the Parameter
	 * @exception  CharacterCodingException  Description of the Exception
	 * @since                                October 31, 2002
	 */
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException;

	/**
	 * @deprecated use {@link #fromByteBuffer(CloverBuffer, CharsetDecoder)} instead
	 */
	@Deprecated
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException;

	/**
	 *  Encode the field's value into ByteBuffer. The numeric value is encoded as a string representation.
	 *
	 * @param  encoder                       Charset encoder which could be used to encode characters
	 * @param  dataBuffer                    Description of the Parameter
	 * @exception  CharacterCodingException  Description of the Exception
	 * @return number of written characters for string based fields ((DataFieldMetadata.isByteBased() == false); byte based fields return 0
	 * @since                                October 31, 2002
	 */
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException;
	
	/**
	 *  Encode the field's value into ByteBuffer. The numeric value is encoded as a string representation.
	 * 
	 * @param dataBuffer buffer where the field's value is written
	 * @param encoder encoder which is used for string to byte conversion
	 * @param maxLength for string based fields (DataFieldMetadata.isByteBased() == false) maximum number of character written to output buffer;
	 * 			byte based fields can ignore this parameter 
	 * @return
	 * @throws CharacterCodingException
	 */
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException;

	/**
	 * @deprecated use {@link #toByteBuffer(CloverBuffer, CharsetEncoder)} instead
	 */
	@Deprecated
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException;

	/**
	 *  Serializes data field into provided byte buffer
	 *
	 * @param  buffer  Description of Parameter
	 * @since          September 16, 2002
	 */
	public void serialize(CloverBuffer buffer);

	/**
	 *  Deserializes data field from provided byte buffer
	 *
	 * @param  buffer  Description of Parameter
	 * @since          September 16, 2002
	 */
	public void deserialize(CloverBuffer buffer);

	/**
	 *  Checks whether two DataField objects are equal. Both
	 * fiels should be of the same type. If they are not, ClassCastException
	 * is thrown.<br> 
	 * Two datafields where each is flagged as to be NULL (isNull() returns true) are
	 * deemed <b>NOT equal</b>.
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       September 16, 2002
	 */
	@Override
	public boolean equals(Object obj);
	
	/**
	 * Answers hash code for this field, obeying general principle:
	 * <code>
	 * field1.equals(field2) == true =&gt; field1.hashCode() == field2.hashCode()
	 * </code>
	 */
	@Override
	public int hashCode();

	/**
	 *  Compares two fields and returs -1,0,1 depending on result of comparison.
	 * Both fiels should be of the same type. If they are not, ClassCastException
	 * is thrown.<br>
	 * When comparing fields where one or both are NULL (isNull() returns true), the
	 * result of comparison is that if <code>obj</code> is NULL the result is 1,
	 * if <code>this.value</code> (value of this field) is NULL, the result is -1; 
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	@Override
	public int compareTo(Object obj);


	/**
	 *  Gets the actual size of field (in bytes).<br>
	 *  <i>How many bytes are required for field serialization</i>
	 * @return    The size in bytes
	 */
	public int getSizeSerialized();
	
}
