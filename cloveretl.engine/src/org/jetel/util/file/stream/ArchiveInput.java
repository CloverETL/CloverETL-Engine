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
package org.jetel.util.file.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.jetel.data.parser.Parser.DataSourceType;

class ArchiveInput extends AbstractInput {
	
	protected final String url;
	protected final InputStream is;

	/**
	 * @param cachedEntry
	 * @param tarStream
	 */
	public ArchiveInput(String url, InputStream is) {
		this.url = url;
		this.is = is;
	}
	
	/**
	 * Archive input stream needs to be wrapped to prevent it from being closed.
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	protected InputStream wrap(InputStream is) throws IOException {
		return new CloseShieldInputStream(is);
	}

	@Override
	public Object getPreferredInput(DataSourceType type) throws IOException {
		switch (type) {
		case STREAM:
		case CHANNEL:
			InputStream wrapper = wrap(is);
			return (type == DataSourceType.STREAM) ? wrapper : Channels.newChannel(wrapper);
		default:
			return null;
		}
	}
	
	@Override
	public String toString() {
		return getAbsolutePath();
	}

	@Override
	public String getAbsolutePath() {
		return url;
	}
	
	
}