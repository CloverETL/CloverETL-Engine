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
package org.jetel.component.tree.writer.xml;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

import org.jetel.component.tree.writer.AttributeWriter;
import org.jetel.component.tree.writer.CDataWriter;
import org.jetel.component.tree.writer.CommentWriter;
import org.jetel.component.tree.writer.NamespaceWriter;
import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.TreeWriter;
import org.jetel.component.tree.writer.model.runtime.WritableMapping;

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

	private final boolean writeXmlDeclaration;

	/**
	 * @param mapping
	 * @param omitNewLines
	 * @param charset
	 * @param version
	 */
	public XmlFormatter(WritableMapping mapping, int maxPortIndex, boolean omitNewLines, String charset, String version, boolean writeXmlDeclaration) {
		super(mapping, maxPortIndex);
		this.charset = charset;
		this.version = version;
		this.omitNewLines = omitNewLines;
		this.writeXmlDeclaration = writeXmlDeclaration;
	}
	
	@Override
	public boolean isListSupported() {
		return false;
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		close();

		if (outputDataTarget instanceof Object[]) {
			Object[] output = (Object[]) outputDataTarget;
			outStream = (OutputStream) output[2];
		} else if (outputDataTarget instanceof WritableByteChannel) {
			outStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
		} else if (outputDataTarget instanceof OutputStream) {
			outStream = (OutputStream) outputDataTarget;
		} else {
			throw new IllegalArgumentException("parameter " + outputDataTarget + " is not instance of WritableByteChannel");
		}
		
		writer = new XmlWriter(outStream, charset, version, omitNewLines, writeXmlDeclaration);
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
		
		// CLO-5977, following try block performs flush and close, ClosedChannelExceptions are eaten (not thrown)
		try {
			flush();
		} catch (ClosedChannelException e) {
			// no action
		} finally {
			try {
				writer.close();
			} catch (ClosedChannelException e) {
				// no action
			}
		}
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

	@Override
	public CDataWriter getCDataWriter() {
		return writer;
	}
}
