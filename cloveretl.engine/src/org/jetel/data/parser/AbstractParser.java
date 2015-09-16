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
package org.jetel.data.parser;

import java.io.IOException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Abstract implementation of {@link Parser} interface.
 * 
 * @author mzatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11 Jan 2012
 */
public abstract class AbstractParser implements Parser {
	
	protected boolean releaseDataSource = true;
	
	@Override
	public void setReleaseDataSource(boolean releaseDataSource) {
		this.releaseDataSource = releaseDataSource;
	}
	
	/**
	 * This method should close the previous data sources
	 * when switching to the next source.
	 */
	protected abstract void releaseDataSource();

	/*
	 * The default implementation does nothing except for
	 * releasing the previous data source.
	 */
	@Override
	public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
		if (releaseDataSource) {
			releaseDataSource();
		}
	}
	
	@Override
	public DataSourceType getPreferredDataSourceType() {
		//channel data source type is preferred by default
		return DataSourceType.CHANNEL;
	}
	
	/**
	 * by default, parser do not support direct reading -i.e. reading serialized data
	 */
	@Override
	public boolean isDirectReadingSupported(){
		return false;
	}
	
	@Override
	public int getNextDirect(CloverBuffer buffer) throws JetelException{
		throw new UnsupportedOperationException("This parser does not support direct reading.");
	}
	
}
