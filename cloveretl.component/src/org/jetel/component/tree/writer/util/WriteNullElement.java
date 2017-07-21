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
package org.jetel.component.tree.writer.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Enum for 'write null element' attribute. Created for CLO-4852.
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11. 3. 2015
 */
public enum WriteNullElement {
	
	TRUE("true"),
	FALSE("false"),
	FALSE_EXCLUDE_IF_INNER_CONTENT_IS_NULL("false - exclude if inner content is null");

	private String value;

	private WriteNullElement(String value) {
		this.value = value;
	}

	public static List<String> getValues() {
		ArrayList<String> list = new ArrayList<>();
		for (WriteNullElement element : values()) {
			list.add(element.value);
		}
		return list;
	}

	public static WriteNullElement fromString(String writeNullString) {
		try {
			if (TRUE.value.equalsIgnoreCase(writeNullString)) {
				return TRUE;
			}
			if (FALSE.value.equalsIgnoreCase(writeNullString)) {
				return FALSE;
			}
			if (FALSE_EXCLUDE_IF_INNER_CONTENT_IS_NULL.value.equalsIgnoreCase(writeNullString)) {
				return FALSE_EXCLUDE_IF_INNER_CONTENT_IS_NULL;
			}
			return valueOf(writeNullString);
		} catch (Exception e) {
			return null;
		}
	}

	public boolean isTrue() {
		return this == TRUE;
	}

}
