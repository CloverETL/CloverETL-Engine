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
package org.jetel.connection.nosql;

import java.util.regex.Matcher;

import org.jetel.data.DataRecord;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 9. 2014
 */
public interface ReplacementHelper {
	
	/**
	 * Creates a {@link Matcher} for the given input string.
	 * 
	 * @param input
	 * @return a new {@link Matcher} for the <code>input</code>
	 */
	public Matcher getMatcher(String input);
	
	/**
	 * Analogous to {@link Matcher#appendReplacement(StringBuffer, String)}.
	 * <ol>
	 * 	<li>Appends to the target {@link StringBuffer} the input starting 
	 * 		from the current {@link Matcher} position up to the beginning of the next match.</li>
	 * 	<li>Appends the replacement value from the referenced field of the {@link DataRecord}.</li>
	 * 	<li>Updates {@link Matcher} position.</li>
	 * </ol>
	 * 
	 * @param sb		target {@link StringBuffer}
	 * @param matcher	the {@link Matcher} used for string substitution
	 * @param values	{@link DataRecord} containing the replacement values
	 * 
	 * @see Matcher#appendReplacement(StringBuffer, String)
	 */
	public void appendReplacement(StringBuffer sb, Matcher matcher, DataRecord values);
	
}
