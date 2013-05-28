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
package org.jetel.database.dbf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.StringDataField;
import org.jetel.util.bytes.FileSeekableByteChannel;
import org.jetel.util.bytes.SeekableByteChannel;
import org.jetel.util.file.FileUtils;

/**
 * Utility class for parsing data from DBF memo file - various types -FoxPro, DBaseIII, DBaseIV.
 * Does not extend from AbstractParser thus can not be used in standard reader. A special component is used.<br>
 * Structure info from http://www.cs.cmu.edu/~varun/cs315p/xbase.txt.
 * 
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 23, 2013
 */
public class DBFMemoParser {

	enum DBFMemoType {
		FOXPRO, DBASE_III, DBASE_IV
	};

	public static final int MAX_MEMO_SIZE = 65536;
	public static final int INITIAL_BUFFER_SIZE = 2048;


	private String charsetName;
	private int memoField;
	private int blocksize;
	private int maxBlockNum;
	private Charset charset;
	private DBFMemoType type;
	private SeekableByteChannel file;
	private MemoParser parser;

	public DBFMemoParser(int memoFieldIdx, DBFMemoType type, String charset) {
		this.type = type;
		this.charsetName = charset;
		this.memoField=memoFieldIdx;
	}

	public void open(URL contextURL, String fileURL) throws IOException {
		charset = Charset.forName(charsetName);
		InputStream stream = FileUtils.getInputStream(contextURL, fileURL);
		if (stream instanceof SeekableByteChannel) {
			file = (SeekableByteChannel) stream;
		} else if (stream instanceof FileInputStream) {
			file = new FileSeekableByteChannel(((FileInputStream) stream).getChannel());
		} else {
			throw new IOException("Input file " + fileURL + " is not a regular file or a seekable stream !");
		}
		switch(type){
		case DBASE_IV:
			parser = new DBaseIVMemoParser(charset,file);
			break;
		case FOXPRO:
			parser = new FoxProMemoParser(charset,file);
		break;
		default:
			throw new IOException("Unsupported memo file type: "+type);
		}
		parser.init();
		
	}
	
	public void close() throws IOException{
		parser=null;
		file.close();
		file = null;
	}
	
	public long memoBlockNumber(Object value){
		if (value instanceof byte[]){
			return parser.parseMemoBlockNumber((byte[])value);
		} else if (value instanceof String) {
			return parser.parseMemoBlockNumber((String)value);
		}
		return -2; // invalid data type
	}
	
	public ByteBuffer getMemoByteBuffer(DataRecord record) throws IOException{
		DataField field=record.getField(memoField);
		if (field instanceof ByteDataField){
			return parser.getMemoBytes(((ByteDataField)field).getValue());
		}else if (field instanceof StringDataField){
			return parser.getMemoBytes(field.toString());
		}
		throw new IOException("Not a DBase memo type DataField: "+field);
		
	}
	
	public CharSequence getMemoString(DataRecord record) throws IOException{
		DataField field=record.getField(memoField);
		if (field instanceof ByteDataField){
			return parser.getMemoString(((ByteDataField)field).getValue());
		}else if (field instanceof StringDataField){
			return parser.getMemoString(field.toString());
		}
		throw new IOException("Not a DBase memo type DataField: "+field);	
		
	}
	
	public byte[] getMemoBytes(DataRecord record) throws IOException{
		ByteBuffer data = getMemoByteBuffer(record);
		byte[] value = new byte[data.remaining()];
		data.get(value);
		return value;
	}
		
	

	private abstract class MemoParser{
		abstract void init() throws IOException;
		abstract ByteBuffer getMemoBytes(long blocknum) throws IOException;
		
		Charset charset;
		SeekableByteChannel file;
		String errorMsg;
		
		MemoParser(Charset charset,SeekableByteChannel file){
			this.charset=charset;
			this.file=file;
		}
		
		String getErrorMsg() {
			return errorMsg;

		}
		
		/**
		 * parse block number from array of ASCII digits.
		 * 0-ASCII code 48, 9-ASCII code 57
		 * 
		 * @param blockNumStr
		 * @return block number parsed or -1 if no value or -2 if error/invalid data
		 */
		long parseMemoBlockNumber(byte[] blockNumStr){
			long blockNum=0;
			boolean valid=false;
			if (blockNumStr.length<1 || blockNumStr.length>10) return -2;
			for(int i=0; i<blockNumStr.length; i++){
				byte digit=blockNumStr[i];
				if (digit>=48 && digit<=57){
					blockNum=blockNum*10 + (digit-48);
					valid=true;
				}else if(digit!=32){
					return -2; // invalid character
				}
			}
			return valid ? blockNum : -1;
		}
		
		/**
		 * parse block number from String
		 * 
		 * @param blockNumStr
		 * @return @return block number parsed or -1 if no value or -2 if error/invalid data
		 */
		long parseMemoBlockNumber(String blockNumStr){
			if (blockNumStr !=null){
				String trimmed=blockNumStr.trim();
				if (trimmed.length()==0) return -1;
				try{
					return Long.parseLong(trimmed);
				}catch(NumberFormatException ex){
					return -2;
				}
			}
			return -2;
		}
		
		ByteBuffer getMemoBytes(byte[] blocknumStr) throws IOException{
			long blocknum = parseMemoBlockNumber(blocknumStr);
			if (blocknum==-1) throw new IOException("Invalid memo block number: "+new String(blocknumStr, "US-ASCII"));
			return getMemoBytes(blocknum);
		}
		
		CharSequence getMemoString(byte[] blocknumStr) throws IOException{
			long blocknum = parseMemoBlockNumber(blocknumStr);
			if (blocknum==-1) throw new IOException("Invalid memo block number: "+new String(blocknumStr, "US-ASCII"));
			return getMemoString(blocknum);
		}
		
		ByteBuffer getMemoBytes(String blocknumStr) throws IOException{
			try{
				long blocknum = Long.parseLong(blocknumStr.trim());
				return getMemoBytes(blocknum);
			}catch(NumberFormatException ex){
				throw new IOException("Invalid memo block number: "+blocknumStr);
			}
		}
		
		CharSequence getMemoString(String blocknumStr) throws IOException{
			try{
				long blocknum = Long.parseLong(blocknumStr.trim());
				return getMemoString(blocknum);
			}catch(NumberFormatException ex){
				throw new IOException("Invalid memo block number: "+blocknumStr);
			}
			
		}
		
		CharSequence getMemoString(long blocknum) throws IOException {
			ByteBuffer result=getMemoBytes(blocknum);
			return charset.decode(result);
		}
		
	}


	private class FoxProMemoParser extends MemoParser{
	
		public static final int FPT_HEADER_SIZE = 512;
		public static final int FPT_HEADER_BLOCK_SIZE_OFFSET = 6;
		public static final int FPT_BLOCK_LENGHT_OFFSET = 4;
		public static final int FPT_BLOCK_HEADER_SIZE =8; 
		ByteBuffer buffer;
		int blocksize,maxBlockNum;
		
		FoxProMemoParser(Charset charset,SeekableByteChannel file){
			super(charset,file);
		}
		
		void init() throws IOException{
			buffer = ByteBuffer.allocate(DBFMemoParser.INITIAL_BUFFER_SIZE);
			buffer.order(ByteOrder.BIG_ENDIAN);
			file.position(0); // seek to the beginning
			buffer.limit(FPT_HEADER_SIZE);
			file.read(buffer);
			buffer.flip();
			blocksize = buffer.getShort(FPT_HEADER_BLOCK_SIZE_OFFSET);
			blocksize = Math.max(1, blocksize);
			maxBlockNum=buffer.getInt(0);
		}
		
		ByteBuffer getMemoBytes(long blocknum) throws IOException{
			file.position(blocknum * blocksize);
			buffer.clear();
			buffer.limit(FPT_BLOCK_HEADER_SIZE);
			file.read(buffer);
			buffer.flip();
			int memolength=buffer.getInt(FPT_BLOCK_LENGHT_OFFSET);
			
			if (memolength> DBFMemoParser.MAX_MEMO_SIZE || memolength <=0) { 
				throw new IOException(String.format("Invalid memo size: %d, block: %d.", memolength,blocknum));
			}
			
			if (buffer.capacity()<memolength){
				buffer = ByteBuffer.allocate(memolength).order(ByteOrder.BIG_ENDIAN);
			}else{
				buffer.clear();
			}
			buffer.limit(memolength);
			file.read(buffer);
			buffer.flip();
			return buffer;
		}
	}
		
	private class DBaseIVMemoParser extends MemoParser {

		public static final int DBT_HEADER_SIZE = 512;
		public static final int DBT_HEADER_BLOCK_SIZE_OFFSET = 20;
		public static final int DBT_BLOCK_LENGHT_OFFSET = 4;
		public static final int DBT_BLOCK_HEADER_SIZE = 8;
		ByteBuffer buffer;
		int blocksize, maxBlockNum;

		DBaseIVMemoParser(Charset charset, SeekableByteChannel file) {
			super(charset,file);
		}

		void init() throws IOException {
			buffer = ByteBuffer.allocate(DBFMemoParser.INITIAL_BUFFER_SIZE);
			buffer.order(ByteOrder.BIG_ENDIAN);
			file.position(0); // seek to the beginning
			buffer.limit(DBT_HEADER_SIZE);
			file.read(buffer);
			buffer.flip();
			blocksize = buffer.getShort(DBT_HEADER_BLOCK_SIZE_OFFSET);
			blocksize = Math.max(1, blocksize);
			maxBlockNum = buffer.getInt(0);
		}

		ByteBuffer getMemoBytes(long  blocknum) throws IOException {
			file.position(blocknum * blocksize);
			buffer.clear();
			buffer.limit(DBT_BLOCK_HEADER_SIZE);
			file.read(buffer);
			buffer.flip();
			int memolength = buffer.getInt(DBT_BLOCK_LENGHT_OFFSET);

			if (memolength > DBFMemoParser.MAX_MEMO_SIZE || memolength <= 0) {
				throw new IOException(String.format("Invalid memo size: %d, block: %d.", memolength, blocknum));
			}

			if (buffer.capacity() < memolength) {
				buffer = ByteBuffer.allocate(memolength).order(ByteOrder.BIG_ENDIAN);
			} else {
				buffer.clear();
			}
			buffer.limit(memolength);
			file.read(buffer);
			buffer.flip();
			return buffer;
		}
	}
}
