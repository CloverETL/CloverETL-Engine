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
package org.jetel.util.spreadsheet;

import org.jetel.util.string.StringUtils;

/**
 * CLO-6995:
 * We want to share this enum in spreadsheet engine and gui
 * but the plugin hierarchy is messed up so the enum has to be here
 * instead of in cloveretl.spreadsheet.commercial plugin.
 * 
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 8. 2015
 */
public enum HyperlinkType {

	NONE("No hyperlink"),
	DOCUMENT("Document"),
	EMAIL("E-mail"),
	FILE("File"),
	URL("URL");
	
	private String label;
	
	private HyperlinkType(String label) {
		this.label = label;
	}
	
	@Override
	public String toString() {
		return label;
	}

	public static HyperlinkType getDefault() {
		return NONE;
	}
	
	public static HyperlinkType valueOfIgnoreCase(String string) {
		for (HyperlinkType type : values()) {
			if (type.toString().equalsIgnoreCase(string)) {
				return type;
			}
		}

		throw new IllegalArgumentException(StringUtils.quote(string) + " is not a valid Hyperlink type");
	}
}
