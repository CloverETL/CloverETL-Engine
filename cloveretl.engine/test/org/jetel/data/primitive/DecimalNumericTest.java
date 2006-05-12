package org.jetel.data.primitive;

import junit.framework.TestCase;
import java.math.*;

public class DecimalNumericTest extends TestCase {
	
	Decimal anInt,aLong,aFloat,aDouble,aDefault,aDoubleIntInt,aDecimalIntInt,anIntInt;

	protected void setUp() throws Exception {
		super.setUp();
		 anInt=DecimalFactory.getDecimal(0);
		 aLong=DecimalFactory.getDecimal(0);
		 aFloat=DecimalFactory.getDecimal(0.00000000001f);
		 aDouble=DecimalFactory.getDecimal(0.00000000000001);
		 aDefault=DecimalFactory.getDecimal();
		 aDoubleIntInt=DecimalFactory.getDecimal(123.456,2,9);
		 aDecimalIntInt=DecimalFactory.getDecimal(aDoubleIntInt,1,6);
		 anIntInt=DecimalFactory.getDecimal(1,2);
	}
	
	public void test_values(){
		assertEquals(0,anInt.getInt());
		assertEquals(Double.valueOf(0),Double.valueOf(anInt.getDouble()));
		assertEquals(0,anInt.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),anInt.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(Integer.MIN_VALUE),anInt.getDecimal());
		assertEquals(new BigDecimal(0),anInt.getBigDecimal());
//		assertEquals(new BigDecimal(0),anInt.getBigDecimalOutput());
		assertEquals(0,anInt.getPrecision());
		assertEquals(0,anInt.getScale());
//		assertEquals(0,anInt.compareTo(DecimalFactory.getDecimal(0)));
		assertFalse(anInt.isNaN());
		anInt.setNaN(true);
		assertTrue(anInt.isNaN());

		assertEquals(0,aLong.getInt());
		assertEquals(Double.valueOf(0),Double.valueOf(aLong.getDouble()));
		assertEquals(0,aLong.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),aLong.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(0),aLong.getDecimal());
		assertEquals(new BigDecimal(0),aLong.getBigDecimal());
//		assertEquals(new BigDecimal(0),aLong.getBigDecimalOutput());
		assertEquals(0,aLong.getPrecision());
		assertEquals(0,aLong.getScale());
//		assertEquals(0,aLong.compareTo(DecimalFactory.getDecimal(0)));
		assertFalse(aLong.isNaN());
		aLong.setNaN(true);
		assertTrue(aLong.isNaN());

		assertEquals(0,aFloat.getInt());
//		assertEquals(Double.valueOf(0.00000000001),Double.valueOf(aFloat.getDouble()));
		assertEquals(0,aFloat.getLong());
//		assertEquals(DecimalFactory.getDecimal(0),aFloat.getDecimal());
		assertNotSame(DecimalFactory.getDecimal(0.00000000001f),aFloat.getDecimal());
//		assertEquals(new BigDecimal("0.00000000001"),aFloat.getBigDecimal());
//		assertEquals(new BigDecimal(0),aFloat.getBigDecimalOutput());
		assertEquals(0,aFloat.getPrecision());
		assertEquals(0,aFloat.getScale());
//		assertEquals(0,aFloat.compareTo(DecimalFactory.getDecimal(0)));
		assertFalse(aFloat.isNaN());
		aFloat.setNaN(true);
		assertTrue(aFloat.isNaN());
}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
