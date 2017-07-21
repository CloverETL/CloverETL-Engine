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
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.Defaults;
import org.jetel.data.tree.formatter.AttributeWriter;
import org.jetel.data.tree.formatter.CommentWriter;
import org.jetel.data.tree.formatter.NamespaceWriter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.exception.JetelException;

/**
 * Simple implementation of streamed xml writer. Does not check validity of output xml.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */
public class XmlWriter implements TreeWriter, NamespaceWriter, AttributeWriter, CommentWriter {

	private OutputStreamWriter outStreamWriter;
	private String charset;
	private String version;
	private CharsetEncoder encoder;

	private boolean omitNewLines;

	//private boolean emptyElement = false;
	private boolean startTagOpened = false;
	private CharStack elementStack = new CharStack();
	private XMLStringBuffer buffer = new XMLStringBuffer(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);

	private static final char[] OPEN_START_TAG = "<".toCharArray();
	private static final char[] OPEN_END_TAG = "</".toCharArray();
	private static final char[] CLOSE_END_TAG = ">".toCharArray();
	private static final char[] CLOSE_EMPTY_ELEMENT = "/>".toCharArray();
	private static final char[] SPACE = " ".toCharArray();
	private static final char[] ASSIGNMENT = "=\"".toCharArray();
	private static final char[] QUOTE = "\"".toCharArray();
	private static final char[] LINE_FEED = "\n".toCharArray();
	private static final char[] COMMENT_START_TAG = "<!-- ".toCharArray();
	private static final char[] COMMENT_END_TAG = " -->".toCharArray();

	private static final char[] NAMESPACE_PREFIX = " xmlns".toCharArray();
	private static final char[] NAMESPACE_DELIMITER = ":".toCharArray();

	public XmlWriter(OutputStream outStream, String charset, String version, boolean omitNewLines) {
		this.charset = charset;
		this.version = version;
		this.encoder = Charset.forName(charset).newEncoder();
		this.omitNewLines = omitNewLines;
		try {
			this.outStreamWriter = new OutputStreamWriter(outStream, charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeStartNode(char[] name) throws JetelException {
		elementStack.setNeedsEndTag();
		elementStack.setShouldIndent();
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines) {
			indent(elementStack.depth);
		}
		
		elementStack.push();

		openStartTag();
		write(name);
	}

	private void openStartTag() throws JetelException {
		startTagOpened = true;
		write(OPEN_START_TAG);
	}

	private void closeStartTag() throws JetelException {
		startTagOpened = false;

		if (elementStack.needsEndTag()) {
			write(CLOSE_END_TAG);
		} else {
			write(CLOSE_EMPTY_ELEMENT);
		}
	}

	@Override
	public void writeAttribute(char[] name, Object value) throws JetelException {
		if (!startTagOpened) {
			throw new JetelException("Attribute not associated with any element");
		}
		write(SPACE);
		write(name);
		write(ASSIGNMENT);
		writeContent(value.toString().toCharArray(), true, true);
		write(QUOTE);
	}

	@Override
	public void writeNamespace(char[] prefix, char[] namespaceURI) throws JetelException {
		if (!startTagOpened) {
			throw new IllegalStateException("Invalid state: start tag is not opened at writeNamespace()");
		}
		write(NAMESPACE_PREFIX);

		if (prefix != null && prefix.length > 0) {
			write(NAMESPACE_DELIMITER);
			write(prefix);
		}

		write(ASSIGNMENT);
		write(namespaceURI);
		write(QUOTE);
	}

	@Override
	public void writeEndNode(char[] name) throws JetelException {
		if (startTagOpened) {
			closeStartTag();
		}

		if (elementStack.needsEndTag()) {
			if (!omitNewLines && elementStack.shouldIndent()) {
				indent(elementStack.depth - 1);
			}

			write(OPEN_END_TAG);
			write(name);
			write(CLOSE_END_TAG);
		}
		elementStack.pop();
	}

	@Override
	public void writeStartTree() throws JetelException {
		writeContent("<?xml version=\"".toCharArray(), false, false);
		writeContent(version.toCharArray(), false, false);
		writeContent("\" encoding=\"".toCharArray(), false, false);
		writeContent(charset.toCharArray(), false, false);
		writeContent("\"?>".toCharArray(), false, false);
	}

	@Override
	public void writeEndTree() throws JetelException {
		if (startTagOpened) {
			closeStartTag();
		}
		if (!elementStack.isEmpty()) {
			throw new IllegalStateException("There are some unclosed elements");
		}
	}

	@Override
	public void writeLeaf(Object value) throws JetelException {
		elementStack.setNeedsEndTag();
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines && elementStack.shouldIndent()) {
			indent(elementStack.depth);
		}

		writeContent(value.toString().toCharArray(), true, false);
	}

	@Override
	public void writeComment(Object content) throws JetelException {
		elementStack.setNeedsEndTag();
		elementStack.setShouldIndent();
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines) {
			indent(elementStack.depth);
		}

		write(COMMENT_START_TAG);
		writeContent(content.toString().toCharArray(), true, false);
		write(COMMENT_END_TAG);
	}

	private void write(char[] content) throws JetelException {
		write(content, 0, content.length);
	}

	private void write(String content) throws JetelException {
		write(content.toCharArray(), 0, content.length());
	}

	private void writeContent(char[] content, boolean escapeChars, boolean escapeDoubleQuotes) throws JetelException {
		if (!escapeChars) {
			write(content);
			return;
		}

		// Index of the next char to be written
		int startWritePos = 0;

		final int end = content.length;

		for (int index = 0; index < end; index++) {
			char ch = content[index];

			if (!encoder.canEncode(ch)) {
				write(content, startWritePos, index - startWritePos);

				// Escape this char as underlying encoder cannot handle it
				write("&#x");
				write(Integer.toHexString(ch));
				write(";");
				startWritePos = index + 1;
				continue;
			}

			switch (ch) {
			case '<':
				write(content, startWritePos, index - startWritePos);
				write("&lt;");
				startWritePos = index + 1;

				break;

			case '&':
				write(content, startWritePos, index - startWritePos);
				write("&amp;");
				startWritePos = index + 1;

				break;

			case '>':
				write(content, startWritePos, index - startWritePos);
				write("&gt;");
				startWritePos = index + 1;

				break;

			case '"':
				write(content, startWritePos, index - startWritePos);
				if (escapeDoubleQuotes) {
					write("&quot;");
				} else {
					write("\"");
				}
				startWritePos = index + 1;

				break;
			}
		}

		// Write any pending data
		write(content, startWritePos, end - startWritePos);
	}

	private void write(char[] data, int offset, int length) throws JetelException {
		try {
			if (length > Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) {
				flushBuffer();
				outStreamWriter.write(data, offset, length);
			} else {
				if (buffer.length + buffer.length > Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) {
					flushBuffer();
				}
				buffer.append(data, offset, length);
			}
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}
	
	public void close() throws IOException {
		outStreamWriter.close();
	}

	public void flush() throws IOException {
		flushBuffer();
		outStreamWriter.flush();
	}

	private void flushBuffer() throws IOException {
		outStreamWriter.write(buffer.ch, buffer.offset, buffer.length);
		buffer.clear();
	}

	private void indent(int depth) throws JetelException {
		char[] indent = new char[(depth << 1) * SPACE.length + LINE_FEED.length];
		System.arraycopy(LINE_FEED, 0, indent, 0, LINE_FEED.length);
		for (int i = LINE_FEED.length; i < indent.length;) {
			System.arraycopy(SPACE, 0, indent, i, SPACE.length);
			i += SPACE.length;
		}
		write(indent);
	}

	private static class CharStack {
		private boolean[] shouldIndent = new boolean[11];
		private boolean[] needsEndTag = new boolean[11];
		private int depth;

		public void push() {
			if (depth == needsEndTag.length) {
				boolean[] newHasChild = new boolean[needsEndTag.length * 2 + 1];
				System.arraycopy(needsEndTag, 0, newHasChild, 0, depth);
				needsEndTag = newHasChild;
				
				boolean[] newShouldIndent = new boolean[needsEndTag.length];
				System.arraycopy(shouldIndent, 0, newShouldIndent, 0, depth);
				shouldIndent = newShouldIndent;
				
			}
			depth++;
			shouldIndent[depth] = false;
			needsEndTag[depth] = false;
		}

		public void pop() {
			depth--;
		}

		public boolean needsEndTag() {
			return needsEndTag[depth];
		}
		
		public void setNeedsEndTag() {
			needsEndTag[depth] = true;
		}
		
		public boolean shouldIndent() {
			return shouldIndent[depth];
		}
		
		public void setShouldIndent() {
			shouldIndent[depth] = true;
		}

		public boolean isEmpty() {
			return depth <= 0;
		}
	}

	private static class XMLStringBuffer {
		char[] ch;
		int offset;
		int length;

		public XMLStringBuffer(int size) {
			ch = new char[size];
		}

		/** Clears the string buffer. */
		public void clear() {
			offset = 0;
			length = 0;
		}

		public void append(char[] ch, int offset, int length) {
			if (ch != null && length > 0) {
				System.arraycopy(ch, offset, this.ch, this.length, length);
				this.length += length;
			}
		}
	}

}
