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
package org.jetel.data;

import org.jetel.graph.runtime.tracker.TokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Token is simple extension of {@link DataRecord}, which is used in jobflow run of a graph.
 * Token identifier is an artificial key which is used for detailed tracking of token.
 * All components use {@link Token}s instead of {@link DataRecord}s in case the graph is running in jobflow mode.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Apr 2012
 * @see TokenTracker
 */
public class Token extends DataRecordImpl {

	private static final long serialVersionUID = -6335039894273092797L;
	
	private static final int TOKEN_ID_LENGTH = Long.SIZE / 8;
	
	private static final byte EMPTY_TOKEN_TAG = 0;
	
	private static final byte NON_EMPTY_TOKEN_TAG = 1;
	
	/** Token identifier which is used for token tracking. */
	private long tokenId = -1;

	/**
	 * Use {@link DataRecordFactory#newRecord(DataRecordMetadata)} instead direct call this constructor.
	 */
	@SuppressWarnings("deprecation")
	protected Token(DataRecordMetadata metadata) {
		super(metadata);
	}

	/**
	 * @return the token identifier which is used for token tracking
	 */
	public long getTokenId() {
		return tokenId;
	}

	/**
	 * Sets token identifier, which is used for token tracking.
	 * @param tokenId the token identifier to set
	 */
	public void setTokenId(long tokenId) {
		this.tokenId = tokenId;
	}

	@Override
	public void serialize(CloverBuffer buffer) {
		serializeTokenId(buffer);
		
		super.serialize(buffer);
	}
	
	@Override
	public void serialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		serializeTokenId(buffer);
		
		super.serialize(buffer,serializer);
	}
	
	@Override
	public void serializeUnitary(CloverBuffer buffer) {
		super.serialize(buffer);
	}
	
	
	@Override
	public void serializeUnitary(CloverBuffer buffer, DataRecordSerializer serializer) {
		super.serialize(buffer,serializer);
	}
	
	@Override
	public void serialize(CloverBuffer buffer, int[] whichFields) {
		serializeTokenId(buffer);
		
		super.serialize(buffer, whichFields);
	}
	
    @Override
	public void serializeUnitary(CloverBuffer buffer,int[] whichFields) {
    	super.serialize(buffer, whichFields);
    }

	private void serializeTokenId(CloverBuffer buffer) {
		serializeTokenId(tokenId, buffer);
	}
	
	/**
	 * Writes tokenId header into given buffer.
	 * @param tokenId token Id written to the buffer
	 * @param buffer target buffer
	 */
	public static void serializeTokenId(long tokenId, CloverBuffer buffer) {
		if (tokenId == -1) {
			buffer.put(EMPTY_TOKEN_TAG);
		} else {
			buffer.put(NON_EMPTY_TOKEN_TAG);
			buffer.putLong(tokenId);
		}
	}
	
	/**
	 * Reads tokenId header from the given buffer.
	 * @param buffer source buffer
	 */
	public static long deserializeTokenId(CloverBuffer buffer) {
		if (buffer.get() == EMPTY_TOKEN_TAG) {
			return -1;
		} else {
			return buffer.getLong();
		}
	}
	
	@Override
	public void deserialize(CloverBuffer buffer) {
		setTokenId(buffer);
		
		super.deserialize(buffer);
	}
	
	@Override
	public void deserialize(CloverBuffer buffer, DataRecordSerializer serializer) {
		setTokenId(buffer);
		
		super.deserialize(buffer,serializer);
	}

	@Override
	public void deserializeUnitary(CloverBuffer buffer) {
		super.deserialize(buffer);
		tokenId = -1;
	}

	@Override
	public void deserializeUnitary(CloverBuffer buffer,DataRecordSerializer serializer) {
		super.deserialize(buffer,serializer);
		tokenId = -1;
	}

	
	@Override
	public void deserialize(CloverBuffer buffer, int[] whichFields) {
		setTokenId(buffer);
		
		super.deserialize(buffer, whichFields);
	}

	private void setTokenId(CloverBuffer buffer) {
		tokenId = deserializeTokenId(buffer);
	}
	
	@Override
	public int getSizeSerialized() {
		return super.getSizeSerialized() + ((tokenId == -1) ? 1 : 1 + TOKEN_ID_LENGTH);
	}

	@Override
	public int getSizeSerializedUnitary() {
		return super.getSizeSerialized();
	}

	@Override
	public void copyFrom(DataRecord fromRecord) {
		super.copyFrom(fromRecord);
		
		//token id needs to be copied as well
		if (fromRecord instanceof Token) {
			setTokenId(((Token) fromRecord).getTokenId());
		}
	}
	
	@Override
	public Token duplicate() {
		DataRecord result = super.duplicate();
		
		//token id needs to be copied as well
		if (result instanceof Token) {
			((Token) result).setTokenId(getTokenId());
		}
		
		return (Token) result;
	}
	
	@Override
	protected DataRecordImpl newInstance(DataRecordMetadata metadata) {
		return new Token(metadata);
	}
	
}
