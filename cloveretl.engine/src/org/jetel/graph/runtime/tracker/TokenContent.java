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
package org.jetel.graph.runtime.tracker;

import org.jetel.data.DataRecord;
import org.jetel.data.Token;
import org.jetel.util.string.StringUtils;

/**
 * This class represents unstructured token content. 
 * TODO this class is only basic skeleton and can be changed in future
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Apr 2012
 */
public class TokenContent extends DataRecordContent {

	//TODO this is just first suggestion how the token content could look like
	
	private long tokenId;
	
	/**
	 * @return token id; negative value means the repersentative object was simple record instead of token
	 */
	public long getTokenId() {
		return tokenId;
	}
	
	@Override
	public String getLabel() {
		String recordLabel = super.getLabel();
		return '#' + Long.toString(tokenId) + (StringUtils.isEmpty(recordLabel) ? "" : (" (" + recordLabel + ")"));
	}

	/**
	 * @param token
	 */
	public void setToken(Token token) {
		super.setRecord(token);
		
		tokenId = (token != null ? token.getTokenId() : -1);
	}
	
	public void setRecord(Token token) {
		setToken(token);
	}
		
	/**
	 * Even basic record can be used for initialization. TokenId is negative.
	 */
	@Override
	public void setRecord(DataRecord record) {
		super.setRecord(record);
		
		tokenId = -1;
	}
	
	/**
	 * Returns {@link #getLabel()}.
	 */
	@Override
	public String toString() {
		return getLabel();
	}
	
}
