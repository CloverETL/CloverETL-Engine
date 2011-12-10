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
package org.jetel.graph.dictionary;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.jetel.exception.ComponentNotReadyException;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 21, 2008
 */
public class WritableChannelDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "writable.channel";

	public WritableChannelDictionaryType() {
		super(TYPE_ID, WritableByteChannel.class);
	}
	
	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if (value == null) {
			return null;
		} else if (value instanceof WritableByteChannel) {
			return value;
		} else if (value instanceof OutputStream) {
			return Channels.newChannel((OutputStream) value);
		} else {
			throw new ComponentNotReadyException(dictionary, "Unknown source type for a Writable channel dictionary type (" + value + ").");
		}
	}
	
	@Override
	public boolean isValidValue(Object value) {
		return value == null || value instanceof WritableByteChannel || value instanceof OutputStream;
	}

}
