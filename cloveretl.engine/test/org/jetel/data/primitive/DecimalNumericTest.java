package org.jetel.data.primitive;

import junit.framework.TestCase;
import java.math.*;

import org.jetel.data.Defaults;

public class DecimalNumericTest extends TestCase {
	
	Decimal anInt,aLong,aFloat,aDouble,aDefault,aDoubleIntInt,aDecimalIntInt,anIntInt;

	protected void setUp() throws Exception {
		super.setUp();
		Defaults.init();
		
		 anInt=DecimalFactory.getDecimal(0);
		 aLong=DecimalFactory.getDecimal((long)Integer.MAX_VALUE*2);
		 aFloat=DecimalFactory.getDecimal(0.00000001f);
		 aDouble=DecimalFactory.getDecimal(0.0000001);
		 aDefault=DecimalFactory.getDecimal();
		 aDoubleIntInt=DecimalFactory.getDecimal(123.456,9,2);
		 aDecimalIntInt=DecimalFactory.getDecimal(aDoubleIntInt,6,1);
		 anIntInt=DecimalFactory.getDecimal(10,4);
	}
	
	public void test_values(){
		for (int i=0;i<5;i++){
			int value=0;
			switch (i) {
				case 1:value=Integer.MIN_VALUE;					
					   break;
				case 2:value=Integer.MAX_VALUE;
					   break;
				case 3:value=123;
					   break;
				case 4:value=-123;
					  break;
			}
			int less=value-1;
			int more=value+1;
		    anInt.setValue(value);
			assertEquals(value,anInt.getInt());
			if (!anInt.isNaN()) assertEquals(new Double(value),new Double(anInt.getDouble()));
			if (!anInt.isNaN()) assertEquals(value,anInt.getLong());
			if (!anInt.isNaN()) assertEquals(DecimalFactory.getDecimal(value),anInt.getDecimal());
			assertNotSame(DecimalFactory.getDecimal(value),anInt.getDecimal());
			assertEquals(10,anInt.getPrecision());
			assertEquals(0,anInt.getScale());
			if (anInt.getInt()==Integer.MAX_VALUE) anInt.setNaN(true);
			if (!anInt.isNaN()) assertEquals(-1,anInt.compareTo(DecimalFactory.getDecimal(more)));
			if (!anInt.isNaN()) assertEquals(-1,anInt.compareTo(new Integer(more)));
			if (!anInt.isNaN()) assertEquals(0,anInt.compareTo(DecimalFactory.getDecimal(value)));
			if (!anInt.isNaN()) assertEquals(0,anInt.compareTo(new Integer(value)));
			if (!anInt.isNaN()) assertEquals(1,anInt.compareTo(DecimalFactory.getDecimal(less)));
			if (!anInt.isNaN()) assertEquals(1,anInt.compareTo(new Integer(less)));
		}

////		assertEquals((long)Integer.MAX_VALUE*2,aLong.getInt());
//		assertEquals(new Double((long)Integer.MAX_VALUE*2),new Double(aLong.getDouble()));
//		assertEquals((long)Integer.MAX_VALUE*2,aLong.getLong());
//		assertEquals(DecimalFactory.getDecimal((long)Integer.MAX_VALUE*2),aLong.getDecimal());
//		assertNotSame(DecimalFactory.getDecimal((long)Integer.MAX_VALUE*2),aLong.getDecimal());
//		assertEquals(8,aLong.getPrecision());
//		assertEquals(2,aLong.getScale());
////		assertEquals(1,aLong.compareTo(DecimalFactory.getDecimal(Integer.MAX_VALUE)));
////		assertEquals(1,aLong.compareTo(new Long(Integer.MAX_VALUE)));
//		assertFalse(aLong.isNaN());
//		aLong.setNaN(true);
//		assertTrue(aLong.isNaN());
//
//		assertEquals(0,aFloat.getInt());
////		assertEquals(Double.valueOf(0.00000000001),Double.valueOf(aFloat.getDouble()));
//		assertEquals(0,aFloat.getLong());
////		assertEquals(DecimalFactory.getDecimal(0),aFloat.getDecimal());
//		assertNotSame(DecimalFactory.getDecimal(0.00000000001f),aFloat.getDecimal());
////		assertEquals(new BigDecimal("0.00000000001"),aFloat.getBigDecimal());
////		assertEquals(new BigDecimal(0),aFloat.getBigDecimalOutput());
//		assertEquals(8,aFloat.getPrecision());
//		assertEquals(2,aFloat.getScale());
////		assertEquals(0,aFloat.compareTo(DecimalFactory.getDecimal(0)));
//		assertFalse(aFloat.isNaN());
//		aFloat.setNaN(true);
//		assertTrue(aFloat.isNaN());
//
//		assertEquals(0,aDouble.getInt());
//		assertEquals(new Double(0),new Double(aDouble.getDouble()));
//		assertEquals(0,aDouble.getLong());
////		assertEquals(DecimalFactory.getDecimal(0),aDouble.getDecimal());
//		assertNotSame(DecimalFactory.getDecimal(0),aDouble.getDecimal());
//		assertEquals(8,aDouble.getPrecision());
//		assertEquals(2,aDouble.getScale());
////		assertEquals(0,aDouble.compareTo(DecimalFactory.getDecimal(0)));
//		assertFalse(aDouble.isNaN());
//		aDouble.setNaN(true);
//		assertTrue(aDouble.isNaN());
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
