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

import org.jetel.data.DataField;
import org.jetel.metadata.DataFieldMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;


/**
 * @author dpavlis
 *
 * Some dBase related constants and conversion routines for
 * dBase to CloverETL type conversions.
 */
public class DBFTypes  {
	
	/*package*/ static final byte[] KNOWN_TYPES={0x02,0x03,0x30,0x43,
												0x63,(byte) 0x83,(byte) 0x8b,(byte) 0x0cb,
												(byte) 0x0f5,(byte) 0x0fb};
	
	/*package*/ static final String[] KNOWN_TYPES_NAMES={
							"FoxBase","FoxBase+","VisualFoxPro","dBaseIV",
							"dBaseIV_systemfile","FoxBase_memo","dBaseIV_memo",
							"dBaseIV_sqltables","FoxPro2.x","FoxBASE"};
	
	public static final String DATA_OFFSET_XML_ATTRIB_NAME="dataOffset";
	public static final String RECORD_SIZE_XML_ATTRIB_NAME="recordSize";
	public static final String DATA_ENCODING_XML_ATTRIB_NAME="charset";
	
	public static final String DATE_FORMAT_MASK="yyyyMMdd";
	
	public static final byte DBF_TYPE_CHARACTER = 'C';
	public static final byte DBF_TYPE_DATE = 'D';
	public static final byte DBF_TYPE_NUMBER = 'N';
	public static final byte DBF_TYPE_LOGICAL = 'L';
	
	
	/**
	 * @param dBase codepage ID
	 * @return Java/Clover ETL codepage name (IANA string)
	 */
	public static String dbfCodepage2Java(byte codepage){
		switch (codepage){
			case 0x00: return "IBM850";
			case 0x01: return "IBM437";
			case 0x02:return "ISO-8859-1";
			case 0x03: return "windows-1252";
			case 0x7D: return "windows-1255";
			case 0x7E: return "windows-1256";
			case 0x64:return "IBM852"; 
			case 0x65:return "IBM865"; 
			case 0x66:return "IBM866"; 
			case 0x67:return "IBM861"; 
			case 0x68:return "KEYBCS2"; 
			case 0x6A:return "IBM737"; 
			case 0x6B:return "IBM857"; 
			case (byte) 0x98:return "x-mac-greek"; 
			case (byte) 0xC8:return "windows-1250";
			case (byte) 0xC9: return "windows-1251";
			case (byte) 0xCB: return "windows-1253";
			case (byte) 0xCA: return "windows-1254";
            default:
                return "IBM850";
		}
	}
	
	public static byte javaCodepage2dbf(String codepage){
		String name=codepage.toUpperCase();
		if (name.equals("IBM850"))
			return 0x02;
		else if (name.equals("IBM437"))
			return 0x01;
		else if (name.equals("ISO-8859-1"))
			return 0x02;
		else if (name.equals("WINDOWS-1252"))
			return 0x03;
		else if (name.equals("WINDOWS-1255"))
			return 0x7d;
		else if (name.equals("WINDOWS-1256"))
			return 0x7e;
		else if (name.equals("IBM852"))
			return 0x64;
		else if (name.equals("IBM865"))
			return 0x65;
		else if (name.equals("IBM866"))
			return 0x66;
		else if (name.equals("IBM861"))
			return 0x67;
		else if (name.equals("KEYBCS2"))
			return 0x68;
		else if (name.equals("IBM737"))
			return 0x6a;
		else if (name.equals("IBM857"))
			return 0x6b;
		else if (name.equals("X-MAC-GREEK"))
			return (byte) 0x98;
		else if (name.equals("WINDOWS-1250"))
			return (byte) 0xC8;
		else if (name.equals("WINDOWS-1251"))
			return (byte) 0xC9;
		else if (name.equals("WINDOWS-1253"))
			return (byte) 0xCB;
		else if (name.equals("WINDOWS-1254"))
			return (byte) 0xCA;
		else
			return 0x02;
		
	}

	/**
	 * @param dBase field type
	 * @return CloverETL field type
	 */
	@SuppressWarnings("DB")
	public static char dbfFieldType2Clover(char type){
		switch(Character.toUpperCase(type)){
			case 'C': //Character
				return DataFieldMetadata.STRING_FIELD;
			case 'N': //Numeric
			case 'O': //Double (dBase Level 7)
				// return DataFieldMetadata.NUMERIC_FIELD;
				return DataFieldMetadata.DECIMAL_FIELD; //Dbase numeric is better represented by decimal
			case 'D': //Date
			case 'T': //DateTime
			case '@': //Timestamp (dBase Level 7)
				return DataFieldMetadata.DATE_FIELD;
			case 'L': //Logical 
				return DataFieldMetadata.BOOLEAN_FIELD;
			case 'M': //Memo
			case 'P': //Picture
				return DataFieldMetadata.BYTE_FIELD;
			case 'I': //Integer
			case '+': //Autoincrement
				return DataFieldMetadata.INTEGER_FIELD;
			default: return DataFieldMetadata.STRING_FIELD;
	        //throw new DBFErrorException("Unsupported DBF field type: \""+String.valueOf(type)+"\" hex: "+Integer.toHexString(type));
		}
	}
	
	public static byte cloverType2dbf(char type){
		switch(type){
		case DataFieldMetadata.STRING_FIELD:
			return 'C'; // C - Character
		case DataFieldMetadata.NUMERIC_FIELD:
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.INTEGER_FIELD:
			return 'N'; // N - Numeric
			/* return 'I'; //Integer */
		case DataFieldMetadata.DATE_FIELD:
		case DataFieldMetadata.DATETIME_FIELD:
			return 'D'; // D - Date
			 /* return 'T'; //DateTime */
		case DataFieldMetadata.BOOLEAN_FIELD:
			return 'L'; // L - Logical
		/* we don't support memos / byte fields */
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			throw new RuntimeException("DBFDataFormatter does not support Clover's BYTE/CBYTE data type.");
		default:
			return 'C'; // C - Character
		}
	}
	
	public static int cloverSize2dbf(DataFieldMetadata field){
		switch(field.getType()){
		case DataFieldMetadata.STRING_FIELD:
			return field.getSize();
		case DataFieldMetadata.NUMERIC_FIELD:
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.INTEGER_FIELD:
			return field.getSize();
			/* return 'I'; //Integer */
		case DataFieldMetadata.DATE_FIELD:
		case DataFieldMetadata.DATETIME_FIELD:
			return  8;	// YYYYMMDD
			 /* return 'T'; //DateTime */
		case DataFieldMetadata.BOOLEAN_FIELD:
			return 1;
		/* we don't support memos / byte fields */
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			throw new RuntimeException("DBFDataFormatter does not support Clover's BYTE/CBYTE data type.");
		default:
			return field.getSize();
		}
	}
	
}
