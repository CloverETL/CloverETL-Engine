package org.jetel.data.primitive;

import junit.framework.TestCase;
import java.math.*;

import org.jetel.data.Defaults;

public class DecimalNumericTest extends TestCase {
	
	Decimal anInt,aLong,aFloat,aDouble,aDefault,aDoubleIntInt,aDecimalIntInt,anIntInt;

	protected void setUp() throws Exception {
		super.setUp();
		Defaults.init();
		
		 anInt=DecimalFactory.getDecimal(Integer.MIN_VALUE);
		 aLong=DecimalFactory.getDecimal(0);
		 aFloat=DecimalFactory.getDecimal(0.00000001f);
		 aDouble=DecimalFactory.getDecimal(0.0000001);
		 aDefault=DecimalFactory.getDecimal();
		 aDoubleIntInt=DecimalFactory.getDecimal(123.456,9,2);
		 aDecimalIntInt=DecimalFactory.getDecimal(aDoubleIntInt,6,1);
		 anIntInt=DecimalFactory.getDecimal(10,4);
	}
	
	public void test_values(){
		assertEquals(Integer.MIN_VALUE,anInt.getInt());
//		assertEquals(new Double(Integer.MIN_VALUE),new Double(anInt.getDouble()));
//		assertEquals(Integer.MIN_VALUE,anInt.getLong());
		assertEquals(DecimalFactory.getDecimal(Integer.MIN_VALUE),anInt.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(Integer.MIN_VALUE),anInt.getDecimal());
		assertEquals(8,anInt.getPrecision());
		assertEquals(2,anInt.getScale());
		assertEquals(-1,anInt.compareTo(DecimalFactory.getDecimal(20)));
		assertEquals(-1,anInt.compareTo(new Integer(20)));
		assertFalse(anInt.isNaN());
		anInt.setNaN(true);
		assertTrue(anInt.isNaN());

//		assertEquals(0,aLong.getInt());
//		assertEquals(new Double(0),new Double(aLong.getDouble()));
//		assertEquals(0,aLong.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),aLong.getDecimal());
//		assertNotSame(DecimalFactory.getDecimal(0),aLong.getDecimal());
//		assertEquals(8,aLong.getPrecision());
//		assertEquals(2,aLong.getScale());
//		assertEquals(0,aLong.compareTo(DecimalFactory.getDecimal(0)));
//		assertEquals(0,anInt.compareTo(new Long(0)));
//		assertFalse(aLong.isNaN());
//		aLong.setNaN(true);
//		assertTrue(aLong.isNaN());
//		assertEquals(new BigDecimal(0),anInt.getBigDecimal());

		assertEquals(0,aFloat.getInt());
//		assertEquals(Double.valueOf(0.00000000001),Double.valueOf(aFloat.getDouble()));
		assertEquals(0,aFloat.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),aFloat.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(0.00000000001f),aFloat.getDecimal());
//		assertEquals(new BigDecimal("0.00000000001"),aFloat.getBigDecimal());
//		assertEquals(new BigDecimal(0),aFloat.getBigDecimalOutput());
		assertEquals(8,aFloat.getPrecision());
		assertEquals(2,aFloat.getScale());
//		assertEquals(0,aFloat.compareTo(DecimalFactory.getDecimal(0)));
		assertFalse(aFloat.isNaN());
		aFloat.setNaN(true);
		assertTrue(aFloat.isNaN());

		assertEquals(0,aDouble.getInt());
		assertEquals(new Double(0),new Double(aDouble.getDouble()));
		assertEquals(0,aDouble.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),aDouble.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(0),aDouble.getDecimal());
		assertEquals(8,aDouble.getPrecision());
		assertEquals(2,aDouble.getScale());
//		assertEquals(0,aDouble.compareTo(DecimalFactory.getDecimal(0)));
		assertFalse(aDouble.isNaN());
		aDouble.setNaN(true);
		assertTrue(aDouble.isNaN());
}

//	public void test_maths(){
//		anInt.add(DecimalFactory.getDecimal(123));
//		assertEquals(new BigDecimal(123),anInt.getBigDecimal());
//		aLong.sub(DecimalFactory.getDecimal(Integer.MAX_VALUE));
//		assertEquals(new BigDecimal(Integer.MIN_VALUE),aLong.getBigDecimal());
//		aDouble.div(anInt);
//		assertEquals(new Double(0.00000001/123),new Double(aDouble.getDouble()));
//	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
