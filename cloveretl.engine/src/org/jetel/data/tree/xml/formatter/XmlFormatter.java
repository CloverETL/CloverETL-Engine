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
package org.jetel.data.tree.xml.formatter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.tree.formatter.AttributeWriter;
import org.jetel.data.tree.formatter.CommentWriter;
import org.jetel.data.tree.formatter.NamespaceWriter;
import org.jetel.data.tree.formatter.TreeFormatter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 2.11.2011
 */
public class XmlFormatter extends TreeFormatter {

	private XmlWriter writer;

	private boolean omitNewLines;
	private String charset;
	private String version;

	private OutputStream outStream;

	/**
	 * @param mapping
	 * @param omitNewLines
	 * @param charset
	 * @param version
	 */
	public XmlFormatter(WritableMapping mapping, int maxPortIndex, boolean omitNewLines, String charset, String version) {
		super(mapping, maxPortIndex);
		this.charset = charset;
		this.version = version;
		this.omitNewLines = omitNewLines;
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		close();

		if (outputDataTarget instanceof WritableByteChannel) {
			outStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
			writer = new XmlWriter(outStream, charset, version, omitNewLines);
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
		writer = null;
	}

	@Override
	public TreeWriter getTreeWriter() {
		return writer;
	}

	@Override
	public NamespaceWriter getNamespaceWriter() {
		return writer;
	}

	@Override
	public AttributeWriter getAttributeWriter() {
		return writer;
	}

	@Override
	public CommentWriter getCommentWriter() {
		return writer;
	}

}
