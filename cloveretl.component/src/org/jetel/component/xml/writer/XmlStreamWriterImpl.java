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
package org.jetel.component.xml.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import javax.xml.stream.XMLStreamException;

/**
 * Simple implementation of streamed xml writer. Does not check validity of output xml.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */
public class XmlStreamWriterImpl {

	private OutputStreamWriter outStreamWriter;
	private String charset;
	private CharsetEncoder encoder;

	private boolean omitNewLines;

	private boolean emptyElement = false;
	private boolean startTagOpened = false;
	private CharStack elementStack = new CharStack();
	private XMLStringBuffer buffer = new XMLStringBuffer(BUFFER_SIZE);

	private static final int BUFFER_SIZE = 8192; 

	public static final char[] CLOSE_START_TAG = ">".toCharArray();
	public static final char[] OPEN_START_TAG = "<".toCharArray();
	public static final char[] OPEN_END_TAG = "</".toCharArray();
	public static final char[] CLOSE_END_TAG = ">".toCharArray();
	public static final char[] CLOSE_EMPTY_ELEMENT = "/>".toCharArray();
	public static final char[] SPACE = " ".toCharArray();
	public static final char[] ASSIGNMENT = "=\"".toCharArray();
	public static final char[] QUOTE = "\"".toCharArray();
	public static final char[] LINE_FEED = "\n".toCharArray();
	public static final char[] COMMENT_START_TAG = "<!-- ".toCharArray();
	public static final char[] COMMENT_END_TAG = " -->".toCharArray();

	public static final char[] NAMESPACE_PREFIX = " xmlns".toCharArray();
	public static final char[] NAMESPACE_DELIMITER = ":".toCharArray();

	public XmlStreamWriterImpl(OutputStream outStream, String charset, boolean omitNewLines) {
		this.charset = charset;
		this.encoder = Charset.forName(charset).newEncoder();
		this.omitNewLines = omitNewLines;
		try {
			this.outStreamWriter = new OutputStreamWriter(outStream, charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public void writeEmptyElement(char[] name) throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines) {
			indent(elementStack.depth);
		}

		openStartTag();
		emptyElement = true;
		elementStack.push(name);

		write(name);
	}

	public void writeStartElement(char[] name) throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines) {
			indent(elementStack.depth);
		}

		openStartTag();
		elementStack.push(name);

		write(name);
	}

	private void openStartTag() throws XMLStreamException {
		startTagOpened = true;
		write(OPEN_START_TAG);
	}

	private void closeStartTag() throws XMLStreamException {
		startTagOpened = false;

		if (emptyElement) {
			write(CLOSE_EMPTY_ELEMENT);
			elementStack.pop();
			emptyElement = false;
		} else {
			write(CLOSE_END_TAG);
		}
	}

	public void writeAttribute(char[] name, String value) throws XMLStreamException {
		if (!startTagOpened) {
			throw new XMLStreamException("Attribute not associated with any element");
		}
		write(SPACE);
		write(name);
		write(ASSIGNMENT);
		writeContent(value.toCharArray(), true, true);
		write(QUOTE);
	}

	public void writeNamespace(char[] prefix, char[] namespaceURI) throws XMLStreamException {
		if (!startTagOpened) {
			throw new IllegalStateException("Invalid state: start tag is not opened at writeNamespace()");
		}
		write(NAMESPACE_PREFIX);

		if (prefix != null) {
			write(NAMESPACE_DELIMITER);
			write(prefix);
		}

		write(ASSIGNMENT);
		write(namespaceURI);
		write(QUOTE);
	}

	public void writeEndElement() throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines && elementStack.hasChild()) {
			indent(elementStack.depth - 1);
		}

		write(OPEN_END_TAG);
		write(elementStack.pop());
		write(CLOSE_END_TAG);
	}

	public void writeStartDocument(String version) throws XMLStreamException {
		writeContent("<?xml version=\"".toCharArray(), false, false);
		writeContent(version.toCharArray(), false, false);
		writeContent("\" encoding=\"".toCharArray(), false, false);
		writeContent(charset.toCharArray(), false, false);
		writeContent("\"?>".toCharArray(), false, false);
	}

	public void writeEndDocument() throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}
		while (!elementStack.isEmpty()) {
			write(OPEN_END_TAG);
			write(elementStack.pop());
			write(CLOSE_END_TAG);
		}
	}

	public void writeCharacters(String content) throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}

		if (!omitNewLines && elementStack.hasChild()) {
			indent(elementStack.depth);
		}

		writeContent(content.toCharArray(), true, false);
	}

	public void writeComment(String content) throws XMLStreamException {
		if (startTagOpened) {
			closeStartTag();
		}
		
		if (!omitNewLines) {
			indent(elementStack.depth);
		}

		write(COMMENT_START_TAG);
		writeContent(content.toCharArray(), true, false);
		write(COMMENT_END_TAG);
	}
	
	private void write(char[] content) throws XMLStreamException {
		write(content, 0, content.length);
	}

	private void write(String content) throws XMLStreamException {
		write(content.toCharArray(), 0, content.length());
	}

	private void writeContent(char[] content, boolean escapeChars, boolean escapeDoubleQuotes)
			throws XMLStreamException {
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

	private void write(char[] data, int offset, int length) throws XMLStreamException {
		try {
			if (length > BUFFER_SIZE) {
				flushBuffer();
				outStreamWriter.write(data, offset, length);
			} else {
				if (buffer.length + buffer.length > BUFFER_SIZE) {
					flushBuffer();
				}
				buffer.append(data, offset, length);
			}
		} catch (IOException e) {
			throw new XMLStreamException(e);
		}
	}

	public void close() throws IOException {
		flush();
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

	private void indent(int depth) throws XMLStreamException {
		char[] indent = new char[(depth << 1) * SPACE.length + LINE_FEED.length];
		System.arraycopy(LINE_FEED, 0, indent, 0, LINE_FEED.length);
		for (int i = LINE_FEED.length; i < indent.length;) {
			System.arraycopy(SPACE, 0, indent, i, SPACE.length);
			i += SPACE.length;
		}
		write(indent);
	}

	private static class CharStack {
		private char[][] stack = new char[10][];
		private boolean[] hasChild = new boolean[11];
		private int depth;

		public void push(char[] data) {
			if (depth == stack.length) {
				char[][] newStack = new char[stack.length * 2][];
				System.arraycopy(stack, 0, newStack, 0, depth);
				stack = newStack;

				boolean[] newHasChild = new boolean[stack.length * 2 + 1];
				System.arraycopy(hasChild, 0, newHasChild, 0, depth);
				hasChild = newHasChild;
			}

			hasChild[depth] = true;
			stack[depth++] = data;
			hasChild[depth] = false;
		}

		public char[] pop() {
			return stack[--depth];
		}

		public boolean hasChild() {
			return hasChild[depth];
		}

		public boolean isEmpty() {
			return depth <= 0;
		}
	}
	
	private class XMLStringBuffer {
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
