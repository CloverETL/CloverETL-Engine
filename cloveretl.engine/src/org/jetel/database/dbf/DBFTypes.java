/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.database.dbf;


/**
 * @author dpavlis
 *
 * Some dBase related constants and conversion routines for
 * dBase to CloverETL type conversions.
 */
public class DBFTypes  {
	
	/*package*/ static final int[] KNOWN_TYPES={0x02,0x03,0x30,0x43,0x63,
										0x83,0x8b,0x0cb,0x0f5,0x0fb};
	
	/*package*/ static final String[] KNOWN_TYPES_NAMES={
							"FoxBase","FoxBase+","VisualFoxPro","dBaseIV",
							"dBaseIV_systemfile","FoxBase_memo","dBaseIV_memo",
							"dBaseIV_sqltables","FoxPro2.x","FoxBASE"};
	
	public static final String DATA_OFFSET_XML_ATTRIB_NAME="dataOffset";
	public static final String RECORD_SIZE_XML_ATTRIB_NAME="recordSize";
	public static final String DATA_ENCODING_XML_ATTRIB_NAME="charset";
	
	public static final String DATE_FORMAT_MASK="yyyyMMdd";
	
	/**
	 * @param dBase codepage ID
	 * @return Java/Clover ETL codepage name (IANA string)
	 */
	public static String dbfCodepage2Java(int codepage){
		switch (codepage){
			case 0x00: return "US-ASCII";
			case 0x01: 
			case 0x02:return "ISO-8859-1";
			case 0x03: return "windows-1252";
			case 0x7D: return "windows-1255";
			case 0x7E: return "windows-1256";
			case 0x64:return "ISO-8859-2"; // TODO: !! this is not 100% correct
			case 0x0C8:return "windows-1250";
			case 0x0C9: return "windows-1251";
			case 0x0CB: return "windows-1253";
			case 0x0CA: return "windows-1254";
            default:
                return "US-ASCII";
		}
	}
	
}
