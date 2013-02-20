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
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

import org.jetel.component.tree.writer.AttributeWriter;
import org.jetel.component.tree.writer.CommentWriter;
import org.jetel.component.tree.writer.NamespaceWriter;
import org.jetel.component.tree.writer.TreeWriter;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelException;

/**
 * Simple implementation of streamed xml writer. Does not check validity of output xml.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */
public class XmlWriter implements TreeWriter, NamespaceWriter, AttributeWriter, CommentWriter {

	private enum TagContent {EMPTY, ATTRIBUTE_COMMENT_NAMESPACE, VALUE_CHILD};
	
	private OutputStreamWriter outStreamWriter;
	private String charset;
	private String version;
	private CharsetEncoder encoder;

	private boolean omitNewLines;

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

	private boolean[] shouldIndentStack = new boolean[11];
	private TagContent[] values = new TagContent[11];
	private char[][] elementNameStack = new char[11][];
	private int depth;
	private int writtenDepth;

	private boolean startTagOpened = false;

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

	public void push(char[] name) {
		if (depth == values.length - 1) {
			int newLength = values.length * 2 + 1;

			values = Arrays.copyOf(values, newLength);
			shouldIndentStack = Arrays.copyOf(shouldIndentStack, newLength);
			elementNameStack = Arrays.copyOf(elementNameStack, newLength);
		}
		shouldIndentStack[depth] = true;
		elementNameStack[depth] = name;

		depth++;
		shouldIndentStack[depth] = false;
	}

	public char[] pop() {
		depth--;
		if (writtenDepth > depth) {
			writtenDepth = depth;
		}
		
		return elementNameStack[depth];
	}

	public boolean shouldIndent() {
		return shouldIndentStack[depth];
	}

	public void setShouldIndent() {
		shouldIndentStack[depth] = true;
	}

	public boolean isEmpty() {
		return depth <= 0;
	}

	private void performDeferredWrite(boolean update, TagContent value) throws JetelException {
		if (update) {
			for (int i = 0; i < depth; i++) {
				values[i] = TagContent.VALUE_CHILD;
			}
			values[depth] = value;
		}
		for (int i = writtenDepth; i < depth; i++) {
			if (startTagOpened) {
				closeStartTag(values[i] == TagContent.VALUE_CHILD);
			}

			if (!omitNewLines) {
				indent(i);
			}

			openStartTag();
			write(elementNameStack[i]);
		}
		writtenDepth = depth;
	}

	@Override
	public void writeStartNode(char[] name) throws JetelException {
		push(name);
		values[depth] = TagContent.EMPTY;
	}

	private void openStartTag() throws JetelException {
		startTagOpened = true;
		write(OPEN_START_TAG);
	}

	/**
	 * @param needsEndTag
	 * @return true if the tag was closed, false otherwise.
	 * @throws JetelException
	 */
	private boolean closeStartTag(boolean needsEndTag) throws JetelException {
		startTagOpened = false;

		if (needsEndTag) {
			write(CLOSE_END_TAG);
			return false;
		} else {
			write(CLOSE_EMPTY_ELEMENT);
			return true;
		}
	}

	@Override
	public void writeAttribute(char[] name, Object value) throws JetelException {
		performDeferredWrite(true, TagContent.ATTRIBUTE_COMMENT_NAMESPACE);

		write(SPACE);
		write(name);
		write(ASSIGNMENT);
		writeContent(value.toString().toCharArray(), true, true);
		write(QUOTE);
	}

	@Override
	public void writeNamespace(char[] prefix, char[] namespaceURI) throws JetelException {
		performDeferredWrite(true, TagContent.ATTRIBUTE_COMMENT_NAMESPACE);

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
	public void writeEndNode(char[] name, boolean writeNullElement) throws JetelException {
		if (writeNullElement || depth == 1 || !isNodeEmpty()) {
			performDeferredWrite(depth != 1, null);
			boolean tagClosed = false;
			if (startTagOpened) {
				tagClosed = closeStartTag(values[depth] == TagContent.VALUE_CHILD);
			}
			if (!tagClosed) {
				if (!omitNewLines && shouldIndent()) {
					indent(depth - 1);
				}

				write(OPEN_END_TAG);
				write(name);
				write(CLOSE_END_TAG);
			}
		}
		pop();
	}
	
	private boolean isNodeEmpty() {
		return values[depth] == TagContent.EMPTY;
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
			closeStartTag(values[depth] == TagContent.VALUE_CHILD);
		}
	}

	@Override
	public void writeLeaf(Object value, boolean writeNullElement) throws JetelException {
		performDeferredWrite(true, TagContent.VALUE_CHILD);
		if (value != null) {
			writeValue(value);
		}
	}

	private void writeValue(Object value) throws JetelException {
		char[] content = value.toString().toCharArray();
		if (content.length == 0) {
			return;
		}
		
		if (startTagOpened) {
			closeStartTag(values[depth] == TagContent.VALUE_CHILD);
		}

		if (!omitNewLines && shouldIndent()) {
			indent(depth);
		}

		writeContent(content, true, false);
	}

	@Override
	public void writeComment(Object content) throws JetelException {
		performDeferredWrite(true, TagContent.ATTRIBUTE_COMMENT_NAMESPACE);

		setShouldIndent();
		if (startTagOpened) {
			closeStartTag(values[depth] == TagContent.VALUE_CHILD);
		}

		if (!omitNewLines) {
			indent(depth);
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
				int codePoint;
				if (!Character.isHighSurrogate(ch)) {
					codePoint = (int)ch;
				} else {
					index++;
					char ch2 = content[index];
					codePoint = Character.toCodePoint(ch, ch2);
				}
				write(Integer.toHexString(codePoint));
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
			throw new JetelException(e);
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
