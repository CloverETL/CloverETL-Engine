
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

package org.jetel.data.formatter;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 *
 */
public class CloverDataFormatter implements Formatter {
	
	DataRecordTape tape;
	DataRecordMetadata metadata;
	DataOutputStream idx;
	long index = 1;

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		tape = new DataRecordTape((String)out,true,false);
		try{
			idx = new DataOutputStream(new FileOutputStream((String)out+".idx"));
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}
		try {
			tape.open();
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}
		tape.addDataChunk();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		try{
			tape.flush(true);
			tape.close();
			idx.flush();
			idx.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public void write(DataRecord record) throws IOException {
		index+=tape.put(record);
		idx.writeLong(index);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		tape.flush(true);
		idx.flush();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		// TODO Auto-generated method stub

	}

}
