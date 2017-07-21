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
package org.jetel.interpreter.data;

import java.math.BigDecimal;

import org.jetel.data.DataField;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;

@SuppressWarnings("unchecked")
public class TLNumericValue<T extends Numeric> extends TLValue implements Numeric {

	public static final TLValue ZERO = new TLNumericValue(TLValueType.INTEGER, new CloverInteger(0));
	public static final TLValue ONE = new TLNumericValue(TLValueType.INTEGER, new CloverInteger(1));
	public static final TLValue MINUS_ONE = new TLNumericValue(TLValueType.INTEGER, new CloverInteger(-1));
	public static final TLValue PI = new TLNumericValue(TLValueType.NUMBER, new CloverDouble(Math.PI));
	public static final TLValue E = new TLNumericValue(TLValueType.NUMBER, new CloverDouble(Math.E));

	private Numeric value;

	public TLNumericValue(TLValueType type) {
		super(type);
		switch (type) {
		case INTEGER:
			value = (T) new CloverInteger(0);
			break;
		case NUMBER:
			value = (T) new CloverDouble(0);
			break;
		case LONG:
			value = (T) new CloverLong(0);
			break;
		case DECIMAL:
			value = (T) DecimalFactory.getDecimal();
			break;
		default:
			throw new RuntimeException("Can't handle value type: " + type);
		}
	}

	public TLNumericValue(TLValueType type, T value) {
		super(type);
		if (!type.isNumeric())
			throw new RuntimeException("Can't handle value type: " + type);
		this.value = value;
	}

	@Override
	public Numeric getValue() {
		return value;
	}

	@Override
	public int getInt() {
		return value.getInt();
	}

	@Override
	public long getLong() {
		return value.getLong();
	}

	@Override
	public double getDouble() {
		return value.getDouble();
	}

	@Override
	public Numeric getNumeric() {
		return value;
	}

	@Override
	public void setValue(Object _value) {
		if (_value instanceof Numeric) {
			setValue((Numeric) _value);
		} else if (_value instanceof Number) {
			setValue((Number) _value);
		} else {
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: " + type);
		}
	}

	@Override
	public void setValue(TLValue _value) {
		if (_value.type.isNumeric()) {
			setValue((Numeric) _value);
		} else {
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: " + type);
		}
	}

	@Override
	public final void setValue(Numeric value) {
		this.value.setValue(value);

	}

	@Override
	public final void setValue(Number value) {
		this.value.setValue(value);
	}

	public void setInt(int value) {
		this.value.setValue(value);
	}

	public void setLong(long value) {
		this.value.setValue(value);

	}

	public void setDouble(double value) {
		this.value.setValue(value);

	}

	@Override
	public int compareTo(TLValue o) {
		if (this.value == null)
			return -1;
		else if (o.getValue() == null)
			return 1;
		if (!o.type.isNumeric())
			throw new IllegalArgumentException("Can't compare value type: " + type + " with type: " + o.type);
		return this.value.compareTo((Numeric) o);
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
	
	
	@Override public boolean equals(Object obj){
		if (this==obj) return true;
		if (obj instanceof TLNumericValue){
			return this.value.equals(((TLNumericValue)obj).value);
		}
		return false;
	}

	@Override
	public void copyToDataField(DataField field) {
		if (field instanceof Numeric) {
			field.setValue(value);
		} else {
			field.fromString(value.toString());
		}

	}

	@Override
	public TLValue duplicate() {
		TLNumericValue<Numeric> newVal = new TLNumericValue<Numeric>(type);
		newVal.value = value.duplicateNumeric();
		return newVal;
	}

	@Override
	public void setValue(DataField field) {
		if (field instanceof Numeric)
			this.value.setValue((Numeric) field);
		else
			throw new IllegalArgumentException("Can't assign value to: " + type + " from DataField: " + field.getMetadata().getTypeAsString());
	}

	@Override
	public String toString() {
		return value.toString();
	}

	@Override
	public void abs() {
		value.abs();
	}

	@Override
	public void add(Numeric a) {
		value.add(a);
	}

	@Override
	public int compareTo(Numeric value) {
		return this.value.compareTo(value);
	}

	@Override
	public void div(Numeric a) {
		value.div(a);

	}

	@Override
	public Numeric duplicateNumeric() {
		return value.duplicateNumeric();
	}

	@Override
	public BigDecimal getBigDecimal() {
		return value.getBigDecimal();
	}

	@Override
	public Decimal getDecimal() {
		return value.getDecimal();
	}

	@Override
	public Decimal getDecimal(int precision, int scale) {
		return value.getDecimal(precision, scale);
	}

	@Override
	public boolean isNull() {
		return value.isNull();
	}

	@Override
	public void mod(Numeric a) {
		value.mod(a);
	}

	@Override
	public void mul(Numeric a) {
		value.mul(a);
	}

	@Override
	public void neg() {
		value.neg();
	}

	@Override
	public void setNull() {
		value.setNull();
	}

	@Override
	public void setValue(int value) {
		this.value.setValue(value);

	}

	@Override
	public void setValue(long value) {
		this.value.setValue(value);
	}

	@Override
	public void setValue(double value) {
		this.value.setValue(value);
	}

	@Override
	public void sub(Numeric a) {
		this.value.sub(a);
	}
}
