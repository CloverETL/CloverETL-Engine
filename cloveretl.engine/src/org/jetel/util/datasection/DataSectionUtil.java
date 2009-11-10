/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.util.datasection;

/**
 * @author admin (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.11.2009
 */
public class DataSectionUtil {

	/**
	 * Encodes a data string to a data section.
	 * @param data
	 * @param dataSection
	 * @return
	 */
	public static String encodeString(String data, DataSection dataSection) {
		// prepare data section
		int conflictDataIndex;
		StringBuilder sb = new StringBuilder();
		
		// append the start
		sb.append(dataSection.getDataSectionStart());
		
		// find conflict data sections
		int start = 0;
		while ((conflictDataIndex = data.indexOf(dataSection.getDataSectionEnd(), start)) != -1) {
			// split conflict data section
			sb.append(data.substring(start, conflictDataIndex+1));	// appends '!' as well
			
			// append the end and return result
			sb.append(dataSection.getDataSectionEnd());
			
			// append the start
			sb.append(dataSection.getDataSectionStart());
			start = conflictDataIndex+1;
		}
		
		// append data
		sb.append(data.substring(start, data.length()));
		
		// append the end and return result
		sb.append(dataSection.getDataSectionEnd());
		return sb.toString();
	}
	
	/**
	 * Removes data section.
	 * pre![CDATA[h![CDATA[e]]]inner![CDATA[]l![CDATA[l]]]![CDATA[]o]]post -->
	 * preh![CDATA[e]]innerl![CDATA[l]]opost
	 * 
	 * @param data
	 * @param dataSection
	 * @return
	 */
	public static String decodeString(String data, DataSection dataSection) {
		// prepare indices
		StringBuilder sb = new StringBuilder();
		int len = dataSection.getDataSectionStart().length();
		int lastAppend = 0;

		// find data section
		int startIndex, endIndex;
		while ((startIndex = data.indexOf(dataSection.getDataSectionStart(), lastAppend)) != -1 && 
				(endIndex = data.indexOf(dataSection.getDataSectionEnd(), startIndex)) > startIndex) {
			// prestring (pre)
			sb.append(data.substring(lastAppend, startIndex));
			
			// data section
			sb.append(data.substring(startIndex + len, endIndex));
			lastAppend = endIndex+2;
		}

		// prestring (pre)
		if (lastAppend == 0) return data;
		sb.append(data.substring(lastAppend, data.length()));
		return sb.toString();
	}
	
	/**
	 * Returns true if the string contains at least one data section.
	 * @param data
	 * @param dataSection
	 * @return
	 */
	public static boolean containsDataSections(String data, DataSection dataSection) {
		int startIndex;
		return ((startIndex = data.indexOf(dataSection.getDataSectionStart())) != -1 && 
				(data.indexOf(dataSection.getDataSectionEnd(), startIndex)) > startIndex);
	}

	/**
	 * Get the first data section.
	 * @param data
	 * @param dataSection
	 * @param from
	 * @return
	 */
	public static String getDataSection(String data, DataSection dataSection, int from) {
		int startIndex, endIndex;
		if (!((startIndex = data.indexOf(dataSection.getDataSectionStart(), from)) != -1 && 
				(endIndex = data.indexOf(dataSection.getDataSectionEnd(), startIndex)) > startIndex)) return null;
		return data.substring(startIndex, endIndex);
	}
	
	/**
	 * Get the first data section block (linked data sections) otherwise null.
	 * @param data
	 * @param dataSection
	 * @param from
	 * @return
	 */
	public static String getDataSectionBlock(String data, DataSection dataSection, int lastAppend) {
		// find data section
		StringBuilder sb = new StringBuilder();
		int startIndex, endIndex;
		boolean found = false;
		while ((startIndex = data.indexOf(dataSection.getDataSectionStart(), lastAppend)) != -1 && 
				(endIndex = data.indexOf(dataSection.getDataSectionEnd(), startIndex)) > startIndex) {
			if (sb.length() > 0 && startIndex != lastAppend) break;
			
			// data section
			sb.append(data.substring(startIndex, endIndex+2));
			lastAppend = endIndex+2;
			found = true;
		}
		return found ? sb.toString() : null;
	}

}
