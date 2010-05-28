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
package org.jetel.component.partition;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class for executing partition function written in CloverETL language
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 30, 2006
 *
 */
public class PartitionTL implements PartitionFunction {

    public static final String INIT_FUNCTION_NAME="init";
    public static final String GETOUTPUTPORT_FUNCTION_NAME="getOutputPort";
    
    
	private WrapperTL wrapper;
	private TransformationGraph graph;

    /**
     * @param srcCode code written in CloverETL language
     * @param metadata
     * @param parameters
     * @param logger
     */
    public PartitionTL(String srcCode, DataRecordMetadata metadata, 
    		Properties parameters, Log logger) {
        wrapper = new WrapperTL(srcCode, metadata, parameters, logger);
    }
    
    /* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#getOutputPort(org.jetel.data.DataRecord)
	 */
	public int getOutputPort(DataRecord record) {
		TLValue value;
		value=wrapper.executePreparedFunction(record, null);
		if (value.type.isNumeric()){
			return ((TLNumericValue)value).getInt();
		}else{
			throw new RuntimeException("Partition - getOutputPort() functions does not return integer value !");
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#init(int, org.jetel.data.RecordKey)
	 */
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException{
		if (graph != null) {
			wrapper.setGraph(graph);
		}
		wrapper.init();
		TLValue params[] = new TLValue[] { TLValue.create(TLValueType.INTEGER)};
		params[0].getNumeric().setValue(numPartitions);
		try {
			wrapper.execute(INIT_FUNCTION_NAME, params);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		wrapper.prepareFunctionExecution(GETOUTPUTPORT_FUNCTION_NAME);
	}

	public TransformationGraph getGraph() {
		return graph;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDirectRecord() {
		return false;
	}

}
