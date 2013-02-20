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
package org.jetel.hadoop.provider.filesystem;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DateDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.NumericDataField;
import org.jetel.data.StringDataField;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;

public class HadoopCloverConvert {

	@SuppressWarnings("rawtypes")
	public static Class cloverType2Hadoop(DataFieldMetadata field) throws IOException{
		switch (field.getDataType()){
		case BOOLEAN:
			return BooleanWritable.class;
		case BYTE:
		case CBYTE:
			return BytesWritable.class;
		case DATE:
			return LongWritable.class;
		case INTEGER:
			return IntWritable.class;
		case LONG:
			return LongWritable.class;
		case NUMBER:
			return DoubleWritable.class;
		case STRING:
			return Text.class;
		default:
			throw new IOException(String.format("Unsupported CloverETL data type \"%s\" of field \"%s\" in conversion to Hadoop.",field.getDataType().getName(),field.getName()));
			
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static DataFieldType hadoopType2Clover( Class data) throws IOException{
		if (data == BooleanWritable.class){
			return DataFieldType.BOOLEAN;
		}else if (data == BytesWritable.class){
			return DataFieldType.BYTE;
		}else if (data == LongWritable.class){
			return DataFieldType.LONG;
		}else if (data == IntWritable.class){
			return DataFieldType.INTEGER;
		}else if (data == DoubleWritable.class){
			return DataFieldType.NUMBER;
		}else if (data == Text.class){
			return DataFieldType.STRING;
		}else{
			throw new IOException(String.format("Unsupported Hadoop data/Class type \"%s\" in conversion from Hadoop to Clover.",data.getName()));
			
		}
	}
	
	
	abstract static class Clover2Hadoop {
		 Object dest;
		 abstract Clover2Hadoop init();
		 abstract void setValue(DataField src);
		 Object getValue() { return dest; }
		 Class<? extends Object> getValueClass() {return dest.getClass();}
	}

	static Clover2Hadoop getC2HCopier(DataFieldMetadata field) throws IOException {
		switch (field.getDataType()){
		case STRING: 
			return (new Clover2Hadoop() {
				@Override
				Clover2Hadoop init(){ dest = new Text(); return this;}
				@Override
				void setValue(DataField src){ ((Text)dest).set(src.toString()); }
			}).init();
		case BYTE:
		case CBYTE:
			return (new Clover2Hadoop() {
				@Override
				Clover2Hadoop init(){ dest = new BytesWritable(); return this;}
				@Override
				void setValue(DataField src){
							final byte val[] = ((ByteDataField)src).getValue();
							((BytesWritable)dest).set(val,0,val.length); }
			}).init();
		case INTEGER:
			return (new Clover2Hadoop(){
				@Override
				Clover2Hadoop init(){ dest = new IntWritable(); return this; }
				@Override
				void setValue(DataField src){ ((IntWritable)dest).set(((IntegerDataField)src).getInt());}
			}).init();
		case LONG:
			return (new Clover2Hadoop(){
				@Override
				Clover2Hadoop init(){ dest = new LongWritable(); return this; }
				@Override
				void setValue(DataField src){ ((LongWritable)dest).set(((LongDataField)src).getLong());}
			}).init();
		case NUMBER:
			return (new Clover2Hadoop(){
				@Override
				Clover2Hadoop init(){ dest = new DoubleWritable(); return this; }
				@Override
				void setValue(DataField src){ ((DoubleWritable)dest).set(((NumericDataField)src).getDouble());}
			}).init();
		case DATE:
			return (new Clover2Hadoop(){
				@Override
				Clover2Hadoop init(){ dest = new LongWritable(); return this;}
				@Override
				void setValue(DataField src){ ((LongWritable)dest).set(((DateDataField)src).getValue().getTime());}
			}).init();
		case BOOLEAN:
			return (new Clover2Hadoop(){
				@Override
				Clover2Hadoop init(){ dest = new BooleanWritable(); return this; }
				@Override
				void setValue(DataField src){ ((BooleanWritable)dest).set(((BooleanDataField)src).getBoolean());}
			}).init();
		default:
			throw new IOException(String.format("Unsupported CloverETL data type \"%s\" of field \"%s\" in conversion to Hadoop.",field.getDataType().getName(),field.getName()));
		}
	}
	
	abstract static class Hadoop2Clover {
		 abstract void copyValue(Writable src, DataField dst);
	}

	@SuppressWarnings("rawtypes")
	static Hadoop2Clover getH2CCopier(Class data) throws IOException {
		if (data == BooleanWritable.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((BooleanDataField)dst).setValue(((BooleanWritable)src).get());}
			});
		}else if (data == BytesWritable.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((ByteDataField)dst).setValue(((BytesWritable)src).getBytes(), 0, ((BytesWritable)src).getLength());}
			});
		}else if (data == LongWritable.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((LongDataField)dst).setValue(((LongWritable)src).get());}
			});
		}else if (data == IntWritable.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((IntegerDataField)dst).setValue(((IntWritable)src).get());}
			});
		}else if (data == DoubleWritable.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((NumericDataField)dst).setValue(((DoubleWritable)src).get());}
			});
		}else if (data == Text.class){
			return (new Hadoop2Clover() {
				@Override
				void copyValue(Writable src,DataField dst){
					((StringDataField)dst).setValue(((Text)src).toString());}
			});
		}else{
			throw new IOException(String.format("Unsupported Hadoop data/Class type \"%s\" in conversion from Hadoop to Clover.",data.getName()));
			
		}
		
	}	
}
