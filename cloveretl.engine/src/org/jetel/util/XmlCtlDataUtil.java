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
package org.jetel.util;

import java.util.ArrayList;
import java.util.List;

/**
 * CTLDATA element support.
 */
public class XmlCtlDataUtil {
	
	private final static String PREFIX = "![CTLDATA[";
	private final static String POSTFIX = "]]";
	
	public static List<XmlData> parseCTLData(String data) {
		List<XmlData> result = new ArrayList<XmlData>();
		int endPos;
		int currentPos = 0;
		while (currentPos != data.length()) {
			if (data.startsWith(PREFIX, currentPos) && (endPos = data.indexOf(POSTFIX, currentPos)) != -1) {
				result.add(new XmlData(data.substring(currentPos + PREFIX.length(), endPos), true));
				currentPos = endPos + POSTFIX.length();
			} else {
				if ((endPos = data.indexOf(PREFIX, currentPos)) != -1 && data.indexOf(POSTFIX, endPos) != -1) {
					result.add(new XmlData(data.substring(currentPos, endPos), false));
					currentPos = endPos;
				} else {
					result.add(new XmlData(data.substring(currentPos), false));
					currentPos = data.length();
				}
			}
		}
		return result;
	}

	/**
	 * Creates CTLDATA element.
	 * @param data
	 * @return
	 */
	public static String createCTLDataElement(String data) {
		return PREFIX + data + POSTFIX;
	}

	/**
	 * Deletes all CTLDATA elements.
	 * @param data
	 * @return
	 */
	public static String deleteAllCTLDataElement(String data) {
		int indEnd = 0;
		StringBuilder sb = new StringBuilder(data);
		while(indEnd != -1) {
			int indStart = sb.indexOf(PREFIX);
			if (indStart == -1) break;
			indEnd = sb.indexOf(POSTFIX, indStart);
			if (indEnd == -1) break;
			sb.delete(indStart, indEnd + POSTFIX.length());
		}
		return sb.toString();
	}
	
	public static class XmlData {
		private String data;
		private boolean isCtlData;

		private XmlData(String data, boolean isCtlData) {
			this.data = data;
			this.isCtlData = isCtlData;
		}
		public String getData() {
			return data;
		}
		public boolean isCTLData() {
			return isCtlData;
		}
	}
}

