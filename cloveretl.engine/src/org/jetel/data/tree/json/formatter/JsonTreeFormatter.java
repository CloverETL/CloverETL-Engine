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
package org.jetel.data.tree.json.formatter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeFormatter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3.1.2012
 */
public class JsonTreeFormatter extends TreeFormatter {
	
	private String encoding;
	private boolean omitNewLines;
	private OutputStream outStream;

	private JsonWriterAdapter writer;

	public JsonTreeFormatter(WritableMapping mapping, int maxPortIndex, String encoding, boolean omitNewLines) {
		super(mapping, maxPortIndex);
		this.encoding = encoding;
		this.omitNewLines = omitNewLines;
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		close();

		if (outputDataTarget instanceof WritableByteChannel) {
			outStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
			writer = new JsonWriterAdapter(outStream, encoding, omitNewLines);
		} else {
			throw new IllegalArgumentException("parameter " + outputDataTarget + " is not instance of WritableByteChannel");
		}

	}

	@Override
	public void flush() throws IOException {
		if (writer != null) {
			writer.flush();
		}
	}

	@Override
	public void close() throws IOException {
		if (writer == null) {
			return;
		}
		
		flush();
		writer.close();
	}

	@Override
	public TreeWriter getTreeWriter() {
		return writer;
	}

	@Override
	public CollectionWriter getCollectionWriter() {
		return writer;
	}

}
