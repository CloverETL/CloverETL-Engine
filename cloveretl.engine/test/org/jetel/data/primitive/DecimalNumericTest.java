package org.jetel.data.primitive;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.jetel.graph.runtime.EngineInitializer;

public class DecimalNumericTest extends TestCase {
	
	Decimal anInt,aLong,aFloat,aDouble,aDefault,aDoubleIntInt,aDecimalIntInt,anIntInt;

	protected void setUp() throws Exception {
		super.setUp();
		EngineInitializer.initEngine(null, null, null);
		
		 anInt=DecimalFactory.getDecimal(0);
		 aLong=DecimalFactory.getDecimal((long)0);
		 aFloat=DecimalFactory.getDecimal(0f);
		 aDouble=DecimalFactory.getDecimal(0);
		 aDefault=DecimalFactory.getDecimal();
		 aDoubleIntInt=DecimalFactory.getDecimal(0,9,2);
		 aDecimalIntInt=DecimalFactory.getDecimal(aDoubleIntInt,6,1);
		 anIntInt=DecimalFactory.getDecimal(12,4);
	}
	
	public void test_values1(){
		System.out.println("\nTests for integer:");
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
	  		   System.out.println("value set to "+value);
			   System.out.println("less set to "+less);
			   System.out.println("more set to "+more);
			assertEquals(value,anInt.getInt());
			System.out.println("Test for getInt passed");
			if (!anInt.isNaN()) assertEquals(new Double(value),new Double(anInt.getDouble()));
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			if (!anInt.isNaN()) assertEquals(value,anInt.getLong());
			System.out.println("Test for getLong passed (isNaN="+anInt.isNaN()+")");
			if (!anInt.isNaN()) assertEquals(DecimalFactory.getDecimal(value),anInt.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
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
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
	}

	public void test_values2(){
		System.out.println("\nTests for long:");
		for (int i=0;i<5;i++){
			long value=0;
			switch (i) {
				case 1:value=(long)Integer.MIN_VALUE;					
					   break;
				case 2:value=(long)Integer.MAX_VALUE;
					   break;
				case 3:value=Long.MIN_VALUE;
					   break;
				case 4:value=Long.MAX_VALUE;
					  break;
			}
			long less=value-1;
			long more=value+1;
  		   System.out.println("value set to "+value);
		   System.out.println("less set to "+less);
		   System.out.println("more set to "+more);
		    aLong.setValue(value);
			if (Integer.MIN_VALUE<=value && value<=Integer.MAX_VALUE) {
				assertEquals(value,aLong.getInt());
				System.out.println("Test for getInt passed");
			}else{
				System.out.println("Test for getInt skipped");
			}
			if (!aLong.isNaN()) assertEquals(new Double(value),new Double(aLong.getDouble()));
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			if (!aLong.isNaN()) assertEquals(value,aLong.getLong());
			System.out.println("Test for getLong passed (isNaN="+anInt.isNaN()+")");
			if (!aLong.isNaN()) assertEquals(DecimalFactory.getDecimal(value),aLong.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
			assertNotSame(DecimalFactory.getDecimal(value),aLong.getDecimal());
			assertEquals(19,aLong.getPrecision());
			assertEquals(0,aLong.getScale());
			if (!aLong.isNaN()&&!(value==Long.MAX_VALUE)) assertEquals(-1,aLong.compareTo(DecimalFactory.getDecimal(more)));
			if (!aLong.isNaN()&&!(value==Long.MAX_VALUE)) assertEquals(-1,aLong.compareTo(new Long(more)));
			if (!aLong.isNaN()) assertEquals(0,aLong.compareTo(DecimalFactory.getDecimal(value)));
			if (!aLong.isNaN()) assertEquals(0,aLong.compareTo(new Long(value)));
			if (!aLong.isNaN()) assertEquals(1,aLong.compareTo(DecimalFactory.getDecimal(less)));
			if (!aLong.isNaN()) assertEquals(1,aLong.compareTo(new Long(less)));
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
	}
	
	public void test_values3(){
		System.out.println("\nTests for float:");
		for (int i=0;i<7;i++){
			float value=0;
			float less=-0.1f;
			float more=0.1f;
			switch (i) {
				case 1:aFloat=DecimalFactory.getDecimal((float)Long.MIN_VALUE);
					   value=Long.MIN_VALUE;
					   less=value-1e19f;
					   break;
				case 2:aFloat=DecimalFactory.getDecimal((float)Long.MAX_VALUE);
					   value=Long.MAX_VALUE;
					   more=(float)Long.MAX_VALUE+1e18f;
					  break;
				case 3:aFloat=DecimalFactory.getDecimal(0.000001f);
					   value=0.000001f;
					   more=0.000002f;
					  break;
				case 4:aFloat=DecimalFactory.getDecimal(-0.000001f);
					   value=-0.000001f;
					   less=-1;
					   break;
				case 5:aFloat=DecimalFactory.getDecimal(Float.MAX_VALUE);
					   value=Float.MAX_VALUE;
				  	   break;
				case 6:aFloat=DecimalFactory.getDecimal(Float.MIN_VALUE);
					   value=Float.MIN_VALUE;
				  	   break;
			}
		   System.out.println("value set to "+value);
		   System.out.println("less set to "+less);
		   System.out.println("more set to "+more);
			if (Integer.MIN_VALUE<=value && value<=Integer.MAX_VALUE) {
				assertEquals(new Float(value).intValue(),aFloat.getInt());
				System.out.println("Test for getInt passed");
			}else{
				System.out.println("Test for getInt skipped");
			}
			if (!aFloat.isNaN()) {
				assertEquals(new Double(value),new Double(aFloat.getDouble()));
			}
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			if (Long.MIN_VALUE<value && value<Long.MAX_VALUE) {
				assertEquals(new Float(value).longValue(),aFloat.getLong());
				System.out.println("Test for getLong passed");
			}else{
				System.out.println("Test for getLong skipped");
			}
			if (!aFloat.isNaN()) assertEquals(DecimalFactory.getDecimal(value),aFloat.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
			assertNotSame(DecimalFactory.getDecimal(value),aFloat.getDecimal());
			if (!aFloat.isNaN()&&!(value==Float.MAX_VALUE)) assertEquals(-1,aFloat.compareTo(DecimalFactory.getDecimal(more)));
			if (!aFloat.isNaN()&&!(value==Float.MAX_VALUE)) assertEquals(-1,aFloat.compareTo(new Double(more)));
			if (!aFloat.isNaN()) assertEquals(0,aFloat.compareTo(DecimalFactory.getDecimal(value)));
			if (!aFloat.isNaN()) assertEquals(0,aFloat.compareTo(new Double(value)));
			if (!aFloat.isNaN()) assertEquals(1,aFloat.compareTo(DecimalFactory.getDecimal(less)));
			if (!aFloat.isNaN()) assertEquals(1,aFloat.compareTo(new Double(less)));
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
	}
	
	public void test_values4(){
	System.out.println("\nTests for double:");
		for (int i=0;i<7;i++){
			double value=0;
			double less=-0.1;
			double more=0.1;
			switch (i) {
				case 1:aDouble=DecimalFactory.getDecimal((double)Long.MIN_VALUE);
					   value=Long.MIN_VALUE;
					   less=value-1e10;
					   break;
				case 2:aDouble=DecimalFactory.getDecimal((double)Long.MAX_VALUE);
					   value=Long.MAX_VALUE;
					   more=value+1e10;
					  break;
				case 3:aDouble=DecimalFactory.getDecimal(Double.MIN_VALUE);
					   value=Double.MIN_VALUE;
					  break;
				case 4:aDouble=DecimalFactory.getDecimal(-Double.MIN_VALUE);
					   value=-Double.MIN_VALUE;
					   break;
				case 5:aDouble=DecimalFactory.getDecimal(Double.MAX_VALUE);
					   value=Double.MAX_VALUE;
				  	   break;
				case 6:aDouble=DecimalFactory.getDecimal(-(Double.MAX_VALUE));
					   value=-(Double.MAX_VALUE);
				  	   break;
			}
		   System.out.println("value set to "+value);
		   System.out.println("less set to "+less);
		   System.out.println("more set to "+more);
			if (Integer.MIN_VALUE<value && value<Integer.MAX_VALUE) {
				assertEquals(new Double(value).intValue(),aDouble.getInt());
				System.out.println("Test for getInt passed");
			}else{
				System.out.println("Test for getInt skipped");
			}
			if (!aDouble.isNaN()) assertEquals(new Double(value),new Double(aDouble.getDouble()));
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			if (Long.MIN_VALUE<value && value<Long.MAX_VALUE) {
				assertEquals(new Double(value).longValue(),aDouble.getLong());
				System.out.println("Test for getLong passed");
			}else{
				System.out.println("Test for getLong skipped");
			}
			if (!aDouble.isNaN()) assertEquals(DecimalFactory.getDecimal(value),aDouble.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
			assertNotSame(DecimalFactory.getDecimal(value),aDouble.getDecimal());
			if (!aDouble.isNaN()&&!(value==Double.MAX_VALUE)) assertEquals(-1,aDouble.compareTo(DecimalFactory.getDecimal(more)));
			if (!aDouble.isNaN()&&!(value==Double.MAX_VALUE)) assertEquals(-1,aDouble.compareTo(new Double(more)));
			if (!aDouble.isNaN()) assertEquals(0,aDouble.compareTo(DecimalFactory.getDecimal(value)));
			if (!aDouble.isNaN()) assertEquals(0,aDouble.compareTo(new Double(value)));
			if (!aDouble.isNaN()&&!(value==-Double.MAX_VALUE)) assertEquals(1,aDouble.compareTo(DecimalFactory.getDecimal(less)));
			if (!aDouble.isNaN()&&!(value==-Double.MAX_VALUE)) assertEquals(1,aDouble.compareTo(new Double(less)));
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
	}
	
	public void test_values5(){
		System.out.println("\nTests for default:");
		for (int i=0;i<5;i++){
			double value=0;
			double d_value=0;
			double less=-0.1;
			double more=0.1;
			switch (i) {
				case 1:value=Double.MIN_VALUE;
					   d_value=0;
					  break;
				case 2:value=-Double.MIN_VALUE;
				       d_value=0;
					   break;
				case 3:value=999999.99;
				       d_value=value;
				       more=999999.999;
				  	   break;
				case 4:value=-999999.99;
			           d_value=value;
			           less=-999999.999;
				  	   break;
			}
			aDefault.setValue(value);
		   System.out.println("value set to "+value);
		   System.out.println("less set to "+less);
		   System.out.println("more set to "+more);
			if (Integer.MIN_VALUE<value && value<Integer.MAX_VALUE) {
				assertEquals(new Double(d_value).intValue(),aDefault.getInt());
				System.out.println("Test for getInt passed");
			}else{
				System.out.println("Test for getInt skipped");
			}
			if (!aDefault.isNaN()) {
				assertEquals(new Double(d_value),new Double(aDefault.getDouble()));
			}
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			if (Long.MIN_VALUE<value && value<Long.MAX_VALUE) {
				assertEquals(new Double(d_value).longValue(),aDefault.getLong());
				System.out.println("Test for getLong passed");
			}else{
				System.out.println("Test for getLong skipped");
			}
			if (!aDefault.isNaN()) assertEquals(DecimalFactory.getDecimal(value,8,2),aDefault.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
			assertNotSame(DecimalFactory.getDecimal(value),aDefault.getDecimal());
			if (!aDefault.isNaN()&&!(value==999999.99)) assertEquals(-1,aDefault.compareTo(DecimalFactory.getDecimal(more)));
			if (!aDefault.isNaN()&&!(value==999999.99)) assertEquals(-1,aDefault.compareTo(new Double(more)));
			if (!(aDefault.isNaN() || aDefault.getDouble()==0)) assertEquals(0,aDefault.compareTo(DecimalFactory.getDecimal(value)));
			if (!(aDefault.isNaN() || aDefault.getDouble()==0)) assertEquals(0,aDefault.compareTo(new Double(value)));
			if (!aDefault.isNaN()&&!(value==-999999.99)) assertEquals(1,aDefault.compareTo(DecimalFactory.getDecimal(less)));
			if (!aDefault.isNaN()&&!(value==-999999.99)) assertEquals(1,aDefault.compareTo(new Double(less)));
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
	}
	
	public void test_values6(){
			System.out.println("\nTests for (9,2):");
			for (int i=0;i<5;i++){
				double value=0;
				double d_value=0;
				double less=-0.1;
				double more=0.1;
				switch (i) {
					case 1:value=Double.MIN_VALUE;
						   d_value=0;
						  break;
					case 2:value=-Double.MIN_VALUE;
					       d_value=0;
						   break;
					case 3:value=9999999.99;
					       d_value=value;
					  	   break;
					case 4:value=-9999999.99;
				           d_value=value;
					  	   break;
				}
				aDoubleIntInt.setValue(value);
			   System.out.println("value set to "+value);
			   System.out.println("less set to "+less);
			   System.out.println("more set to "+more);
				if (Integer.MIN_VALUE<value && value<Integer.MAX_VALUE) {
					assertEquals(new Double(d_value).intValue(),aDoubleIntInt.getInt());
					System.out.println("Test for getInt passed");
				}else{
					System.out.println("Test for getInt skipped");
				}
				if (!aDoubleIntInt.isNaN()) assertEquals(new Double(d_value),new Double(aDoubleIntInt.getDouble()));
				System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
				if (Long.MIN_VALUE<value && value<Long.MAX_VALUE) {
					assertEquals(new Double(d_value).longValue(),aDoubleIntInt.getLong());
					System.out.println("Test for getLong passed");
				}else{
					System.out.println("Test for getLong skipped");
				}
				if (!aDoubleIntInt.isNaN()) assertEquals(DecimalFactory.getDecimal(value,9,2),aDoubleIntInt.getDecimal());
				System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
				assertNotSame(DecimalFactory.getDecimal(value),aDoubleIntInt.getDecimal());
				if (!aDoubleIntInt.isNaN()&&!(value==9999999.99)) assertEquals(-1,aDoubleIntInt.compareTo(DecimalFactory.getDecimal(more)));
				if (!aDoubleIntInt.isNaN()&&!(value==9999999.99)) assertEquals(-1,aDoubleIntInt.compareTo(new Double(more)));
				if (!(aDoubleIntInt.isNaN() || aDoubleIntInt.getDouble()==0)) assertEquals(0,aDoubleIntInt.compareTo(DecimalFactory.getDecimal(value)));
				if (!(aDoubleIntInt.isNaN() || aDoubleIntInt.getDouble()==0)) assertEquals(0,aDoubleIntInt.compareTo(new Double(value)));
				if (!aDoubleIntInt.isNaN()&&!(value==-9999999.99)) assertEquals(1,aDoubleIntInt.compareTo(DecimalFactory.getDecimal(less)));
				if (!aDoubleIntInt.isNaN()&&!(value==-9999999.99)) assertEquals(1,aDoubleIntInt.compareTo(new Double(less)));
				System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
			}
	}
	
	public void test_values7(){
			System.out.println("\nTests for (6,1) from Decimal:");
			for (int i=0;i<4;i++){
				switch (i) {
				case 0:aDoubleIntInt=DecimalFactory.getDecimal(0,9,2);
					  break;
				case 1:aDoubleIntInt=DecimalFactory.getDecimal(0,9,0);
				  break;
				case 2:aDoubleIntInt=DecimalFactory.getDecimal(0.1,9,2);
				  break;
				case 3:aDoubleIntInt=DecimalFactory.getDecimal(111.1,4,1);
				  break;
			}
				aDecimalIntInt=DecimalFactory.getDecimal(aDoubleIntInt,6,1);
				System.out.println("Oryginal decimal:"+aDoubleIntInt.toString());
				System.out.println("My decimal:"+aDecimalIntInt.toString());
				assertEquals(aDoubleIntInt.getInt(),aDecimalIntInt.getInt());
				System.out.println("Test for getInt passed");
				assertEquals(new Double(aDoubleIntInt.getDouble()),new Double(aDecimalIntInt.getDouble()));
				System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
				assertEquals(aDoubleIntInt.getLong(),aDecimalIntInt.getLong());
				System.out.println("Test for getLong passed");
				assertEquals(DecimalFactory.getDecimal(aDoubleIntInt,6,1),aDecimalIntInt.getDecimal());
				System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
				assertNotSame(DecimalFactory.getDecimal(aDoubleIntInt,6,1),aDecimalIntInt.getDecimal());
				assertEquals(-1,aDecimalIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble()+0.2,6,1)));
				assertEquals(-1,aDecimalIntInt.compareTo(new Double(aDoubleIntInt.getDouble()+0.2)));
				assertEquals(0,aDecimalIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble(),6,1)));
				assertEquals(0,aDecimalIntInt.compareTo(new Double(aDoubleIntInt.getDouble())));
				assertEquals(1,aDecimalIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble()-0.1,6,1)));
				assertEquals(1,aDecimalIntInt.compareTo(new Double(aDoubleIntInt.getDouble()-0.1)));
				System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
			}
			
		}
		
	public void test_values8(){
		System.out.println("\nTests for HugeDecimal:");
		for (int i=0;i<1;i++){
			double value=0;
			switch (i) {
			case 0:value=0;
				  break;
			case 1:aDoubleIntInt=DecimalFactory.getDecimal(0,9,0);
			  break;
			case 2:aDoubleIntInt=DecimalFactory.getDecimal(0.1,9,2);
			  break;
			case 3:aDoubleIntInt=DecimalFactory.getDecimal(111.1,4,1);
			  break;
		}
			anIntInt.setValue(value);
			System.out.println("value="+value);
			System.out.println("decimal:"+anIntInt.toString());
			assertEquals(new Double(value).intValue(),anIntInt.getInt());
			System.out.println("Test for getInt passed");
			assertEquals(new Double(aDoubleIntInt.getDouble()),new Double(anIntInt.getDouble()));
			System.out.println("Test for getDouble passed (isNaN="+anInt.isNaN()+")");
			assertEquals(aDoubleIntInt.getLong(),anIntInt.getLong());
			System.out.println("Test for getLong passed");
			assertEquals(DecimalFactory.getDecimal(aDoubleIntInt,6,1),anIntInt.getDecimal());
			System.out.println("Test for getDecimal passed (isNaN="+anInt.isNaN()+")");
			assertNotSame(DecimalFactory.getDecimal(aDoubleIntInt,6,1),anIntInt.getDecimal());
			assertEquals(-1,anIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble()+0.2,6,1)));
			assertEquals(-1,anIntInt.compareTo(new Double(aDoubleIntInt.getDouble()+0.2)));
			assertEquals(0,anIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble(),6,1)));
			assertEquals(0,anIntInt.compareTo(new Double(aDoubleIntInt.getDouble())));
			assertEquals(1,anIntInt.compareTo(DecimalFactory.getDecimal(aDoubleIntInt.getDouble()-0.1,6,1)));
			assertEquals(1,anIntInt.compareTo(new Double(aDoubleIntInt.getDouble()-0.1)));
			System.out.println("Test for compareTo passed (isNaN="+anInt.isNaN()+")");
		}
		
	}
	
	public void test_compare(){
		CloverInteger cint = new CloverInteger(1);
		CloverDouble cdouble = new CloverDouble(1.5);
		System.out.println(cint.compareTo(cdouble));
		System.out.println(cdouble.compareTo(cint));
	}

	public void test_maths_add(){
		aDouble=DecimalFactory.getDecimal(0,6,2);
		anInt=DecimalFactory.getDecimal(123,6,3);
		aDouble.add(anInt);
		assertEquals(new Double(123),new Double(aDouble.getDouble()));
		double value=(long)Integer.MIN_VALUE-100;
		aDouble=DecimalFactory.getDecimal(0,6,2);
		aLong=DecimalFactory.getDecimal(value,20,2);
		aLong.add(aDouble);
		assertEquals(new Double(value),new Double(aLong.getDouble()));
	}
	
	public void test_maths_div(){
		aDouble=DecimalFactory.getDecimal(-0.1,6,2);
		aDouble.abs();
		assertEquals(new Double(0.1),new Double(aDouble.getDouble()));
		aDouble.abs();
		assertEquals(new Double(0.1),new Double(aDouble.getDouble()));
		anInt=DecimalFactory.getDecimal(2,6,3);
		aDouble.div(anInt);
		assertEquals(new Double(0.05),new Double(aDouble.getDouble()));
		aLong=DecimalFactory.getDecimal((long)Integer.MAX_VALUE+10);
		aDouble.div(aLong);
		assertEquals(new Double(0),new Double(aDouble.getDouble()));
		aDouble=DecimalFactory.getDecimal(1,6,2);
		aLong.neg();
		aDouble.div(aLong);
		assertEquals(new Double(0),new Double(aDouble.getDouble()));
		
		CloverDouble d1 = new CloverDouble(0);
		CloverDouble d2 = new CloverDouble(0.01);
		d1.div(d2);
		assertEquals(new CloverDouble(0), d1);
		d1.div(d2.getDecimal());
		assertEquals(new CloverDouble(0), d1);
	}

	public void test_maths_mod(){
		aDouble=DecimalFactory.getDecimal(10,6,2);
		anInt=DecimalFactory.getDecimal(3,6,3);
		aDouble.mod(anInt);
		assertEquals(new Double(1),new Double(aDouble.getDouble()));
		aDouble=DecimalFactory.getDecimal(10,6,2);
		anInt=DecimalFactory.getDecimal(3);
		aDouble.mod(anInt);
		assertEquals(new Double(1),new Double(aDouble.getDouble()));
		aDouble=DecimalFactory.getDecimal(10);
		anInt=DecimalFactory.getDecimal(3,6,2);
		aDouble.mod(anInt);
		assertEquals(new Double(1),new Double(aDouble.getDouble()));
	}

	public void test_maths_mul(){
		System.out.println("\nTests for multipling:");
		aDouble=DecimalFactory.getDecimal(0.1,6,2);
		anInt=DecimalFactory.getDecimal(3,6,3);
		aDouble.mul(anInt);
		assertEquals(new Double(0.3),new Double(aDouble.getDouble()));
		aDouble.neg();
		assertEquals(new Double(-0.3),new Double(aDouble.getDouble()));
		aDouble.mul(DecimalFactory.getDecimal(0));
		assertEquals(new Double(0),new Double(aDouble.getDouble()));
	}

	public void test_maths_sub(){
		aDouble=DecimalFactory.getDecimal(0,6,2);
		anInt=DecimalFactory.getDecimal(123,6,3);
		aDouble.sub(anInt);
		assertEquals(new Double(-123),new Double(aDouble.getDouble()));
		aLong=DecimalFactory.getDecimal((long)Integer.MAX_VALUE+100);
		aLong.sub(anInt);
		assertEquals(new Double(Integer.MAX_VALUE-23),new Double(aLong.getDouble()));
		aDouble=DecimalFactory.getDecimal(0,6,2);
		anInt=DecimalFactory.getDecimal(0,25,0);
		aDouble.sub(anInt);
		assertEquals(new Double(0),new Double(aDouble.getDouble()));
	}

	public void test_fromString(){
		aDouble.fromString("123.45",null);
		assertEquals(new Double(123),new Double(aDouble.getDouble()));
		aDouble=DecimalFactory.getDecimal(10,5);
		aDouble.fromString(".12345",null);
		assertEquals(new Double(0.12345),new Double(aDouble.getDouble()));
		aDouble.fromString("12345",null);
		assertEquals(new Double(12345),new Double(aDouble.getDouble()));
	}

	public void test_s_ds(){
		System.out.println("\nTests for serialization and deserilization:");
		aDouble=DecimalFactory.getDecimal(0.2,6,2);
		anInt=DecimalFactory.getDecimal(123,6,3);
		aLong=DecimalFactory.getDecimal((long)Integer.MAX_VALUE+10,20,2);
        Decimal d=DecimalFactory.getDecimal(aDouble.getPrecision(), aDouble.getScale());
        Decimal i=DecimalFactory.getDecimal(anInt.getPrecision(), anInt.getScale());
        Decimal l=DecimalFactory.getDecimal(aLong.getPrecision(), aLong.getScale());
		ByteBuffer bb=ByteBuffer.allocate(1000);
		aDouble.serialize(bb);
		anInt.serialize(bb);
		aLong.serialize(bb);
		bb.rewind();
		d.deserialize(bb);
		System.out.println("Oryginal:"+aDouble.toString()+",copy:"+d.toString());
		i.deserialize(bb);
		System.out.println("Oryginal:"+anInt.toString()+",copy:"+i.toString());
		l.deserialize(bb);
		System.out.println("Oryginal:"+aLong.toString()+",copy:"+l.toString());
		assertEquals(aDouble,d);
		assertEquals(anInt,i);
		assertEquals(aLong,l);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
