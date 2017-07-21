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

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Token;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This edge implementation is used in case the graph is running with jobflow {@link GraphNature}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 May 2012
 * 
 * @see EdgeFactory
 */
public class JobflowEdge extends Edge {

	private Token lastWrittenToken;
	
	private Token lastReadToken;
	
	public JobflowEdge(String id, DataRecordMetadata metadata, boolean debugMode) {
        super(id, metadata, debugMode);
	}

    public JobflowEdge(String id, DataRecordMetadata metadata) {
        super(id, metadata);
    }
    
	public JobflowEdge(String id, DataRecordMetadataStub metadataStub) {
		super(id, metadataStub);
	}

	public JobflowEdge(String id, DataRecordMetadataStub metadataStub, DataRecordMetadata metadata, boolean debugMode) {
		super(id, metadataStub, metadata, debugMode);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		lastWrittenToken = DataRecordFactory.newToken(getMetadata());
		lastReadToken = DataRecordFactory.newToken(getMetadata());
	}
	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		if (!(record instanceof Token)) {
			throw new IllegalArgumentException("only tokens can be passed to jobflow edge");
		}
		getWriter().getTokenTracker().writeToken(getOutputPortNumber(), record);
		lastWrittenToken.copyFrom(record);
		
		getWriter().setResultCode(Result.WAITING, Result.RUNNING);
		super.writeRecord(record);
		getWriter().setResultCode(Result.RUNNING, Result.WAITING);
	}
	
	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		lastWrittenToken.deserialize(record);
		record.rewind();
		getWriter().getTokenTracker().writeToken(getOutputPortNumber(), lastWrittenToken);
		
		getWriter().setResultCode(Result.WAITING, Result.RUNNING);
		super.writeRecordDirect(record);
		getWriter().setResultCode(Result.RUNNING, Result.WAITING);
	}
	
	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		getReader().setResultCode(Result.WAITING, Result.RUNNING);
		DataRecord result = super.readRecord(record);
		getReader().setResultCode(Result.RUNNING, Result.WAITING);
		
		if (result != null) {
			getReader().getTokenTracker().readToken(getInputPortNumber(), record);
			lastReadToken.copyFrom(record);
		} else {
			getReader().getTokenTracker().eofInputPort(getInputPortNumber());
		}
		
		return result;
	}
	
	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		getReader().setResultCode(Result.WAITING, Result.RUNNING);
		boolean result = super.readRecordDirect(record);
		getReader().setResultCode(Result.RUNNING, Result.WAITING);
		
		if (result) {
			lastReadToken.deserialize(record);
			record.rewind();
			getReader().getTokenTracker().readToken(getInputPortNumber(), lastReadToken);
		} else {
			getReader().getTokenTracker().eofInputPort(getInputPortNumber());
		}

		return result;
	}
	
	@Override
	public void eof() throws InterruptedException, IOException {
		super.eof();
		getWriter().getTokenTracker().eofOutputPort(getOutputPortNumber());
	}
	
	@Override
	public EdgeTypeEnum getEdgeType() {
		//jobflow prefers fast propagate edge type 
		return edgeType != null ? edgeType : EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
	}

}
