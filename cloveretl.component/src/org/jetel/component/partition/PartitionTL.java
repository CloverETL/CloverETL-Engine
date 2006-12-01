
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.component.partition;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.component.RecordTransformTL;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.node.CLVFFunctionDeclaration;
import org.jetel.interpreter.node.CLVFStart;
import org.jetel.metadata.DataRecordMetadata;

/**
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
    
    
    private DataRecordMetadata metadata;
    private String srcCode;
    private Log logger;
	private Properties parameters;
	private WrapperTL wrapper;

    public PartitionTL(String srcCode, DataRecordMetadata metadata, 
    		Properties parameters, Log logger) {
        this.srcCode=srcCode;
        this.logger=logger;
        this.parameters = parameters;
        this.metadata = metadata;
        wrapper = new WrapperTL(srcCode, metadata, parameters, logger);
    }

    /* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#getOutputPort(org.jetel.data.DataRecord)
	 */
	public int getOutputPort(DataRecord record) {
		return ((CloverInteger)wrapper.execute("getOutputPort", record)).getInt();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#init(int, org.jetel.data.RecordKey)
	 */
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException{
		wrapper.init();
		wrapper.execute("init");
	}

}
