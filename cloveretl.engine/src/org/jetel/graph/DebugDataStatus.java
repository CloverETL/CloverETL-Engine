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
package org.jetel.graph;

import org.jetel.exception.JetelRuntimeException;

/**
 * This class represents a progress/final status of edge/component debugging.
 * {@link #INCOMPLETE_DATA} 
 * {@link #COMPLETE_DATA} means that the last records in debug data stream
 * is the last record going through the edge; records can be skipped in the middle of the stream
 * {@link #INCOMPLETE_DATA} means that the last records in debug data stream
 * is not the last record going through the edge
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 4. 2017
 */
public enum DebugDataStatus {

	/**
	 * This constant means that the debugging is still performing or
	 * some other data records are still available in debug data stream.
	 */
	INCOMPLETE_DATA(-1, false),
	
	/**
	 * This constant means that the last record in debug data stream
	 * has been reached and no following data records used to be available
	 * on the edge or component. So the last records in debug data stream
	 * is actually the really last record going through the edge.
	 * Some records can be still skipped in the middle of the stream.
	 */
	COMPLETE_DATA(-2, true),

	/**
	 * This constant means that the last record in debug data stream
	 * has been reached but some data records used to be available
	 * on the edge or component. So the last records in debug data stream
	 * is NOT actually the really last record going through the edge.
	 * Some records can be still skipped in the middle of the stream.
	 * This constant is used by designer to show indication that the 
	 * data records stream is not complete, but no more data records
	 * has been stored in debug edge cache.
	 */
	TRUNCATED_DATA(-3, true);

	/**
	 * This code is used by debug data serialization algorithm.
	 * See {@link EdgeDebugWriter}.
	 */
	private long code;
	
	/**
	 * This is just flag, which indicates no more data records
	 * are available in debug data stream.
	 */
	private boolean isNoMoreData;
	
	private DebugDataStatus(long code, boolean isNoMoreData) {
		this.code = code;
		this.isNoMoreData = isNoMoreData;
	}
	
	public long getCode() {
		return code;
	}
	
	public boolean isNoMoreData() {
		return isNoMoreData;
	}
	
	public static DebugDataStatus fromCode(long code) {
		for (DebugDataStatus status : values()) {
			if (status.code == code) {
				return status;
			}
		}
		throw new JetelRuntimeException("Unexpected debug status code: " + code);
	}

	public static DebugDataStatus fromNoMoreData(boolean noMoreData) {
		return noMoreData ? DebugDataStatus.COMPLETE_DATA : INCOMPLETE_DATA;
	}

}
