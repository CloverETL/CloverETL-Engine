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
package org.jetel.connection.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Splits SQL script into series of SQL statements.
 * 
 * <ul>
 * <li>Ignores delimiter in comments and strings.
 * <li>Works on {@link String}s, {@link InputStream}s and {@link ReadableByteChannel}s.
 * <li>Removes single-line comments.
 * <li>Keeps multi-line comments
 * <li>Supports optimization hint comments /*! MySQL code here {@literal *}{@literal /}
 *     or /*+ Oracle code here {@literal *}{@literal /}
 * <li>Does not recognize MySQL double-quote (") strings
 * <li>Supports MySQL quote escaping (\')
 * <li>Strips away whitespace at the beginning of the statement.
 * <li>Throws {@link IOException} when input ends unexpectedly or when the underline data source throws IOException.
 * </ul>
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
public class SQLScriptParser implements Iterable<String> {


	private static final Log logger = LogFactory.getLog(SQLScriptParser.class);
	
	public static final String DEFAULT_SQL_DELIMITER = ";";
	
	private Reader reader; // always supports mark() and reset()
	private String delimiter = DEFAULT_SQL_DELIMITER;
	private boolean requireLastDelimiter = true;
	private boolean backslashQuoteEscaping = false; // MySQL feature
	
	private final static char QUOTE_CHAR = '\'';
	private static final String DOUBLE_QUOTE = "''";
	private static final String BACKSLASH_ESCAPED_QUOTE = "\\'";
	private final static String MULTILINE_COMMENT_START = "/*";
	private final static String MULTILINE_COMMENT_END = "*/";
	private final static String SINGLELINE_COMMENT_START = "--";
	
	public SQLScriptParser() {
	}
	
	public SQLScriptParser(String string, String delimiter) {
		setStringInput(string);
		this.delimiter = delimiter;
	}

	public SQLScriptParser(InputStream stream, Charset charset, String delimiter) {
		setStreamInput(stream, charset);
		this.delimiter = delimiter;
	}

	public SQLScriptParser(ReadableByteChannel channel, Charset charset, String delimiter) {
		setChannelInput(channel, charset);
		this.delimiter = delimiter;
	}

	public void setInput(Object input, Charset charset) {
		if (input instanceof String) {
			setStringInput((String) input);
		}
		else if (input instanceof InputStream) {
			setStreamInput((InputStream) input, charset);
		}
		else if (input instanceof ReadableByteChannel) {
			setChannelInput((ReadableByteChannel) input, charset);
		}
		else {
			throw new IllegalArgumentException("Supplied input " + input.getClass().getName() + " is not supported");
		}
	}
	
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	
	public String getDelimiter() {
		return delimiter;
	}
	
	/**
	 * When enabled will not fail when last statement is not terminated with semicolon.
	 */
	public void setRequireLastDelimiter(boolean requireLastDelimiter) {
		this.requireLastDelimiter = requireLastDelimiter;
	}
	
	public boolean isRequireLastDelimiter() {
		return requireLastDelimiter;
	}
	
	/**
	 * When enabled, \' is treated as escaped quote in string and does not terminate string parsing.
	 */
	public void setBackslashQuoteEscaping(boolean backslashQuoteEscaping) {
		this.backslashQuoteEscaping = backslashQuoteEscaping;
	}
	
	public boolean isBackslashQuoteEscaping() {
		return backslashQuoteEscaping;
	}
	
	public void setReader(Reader reader) {
		this.reader = reader;
	}

	public void setStringInput(String string) {
		this.reader = new StringReader(string);
	}

	public void setStreamInput(InputStream stream, Charset charset) {
		InputStreamReader inputStreamReader = new InputStreamReader(stream, charset);
		this.reader = new BufferedReader(inputStreamReader); // add mark() and reset() support
	}

	public void setChannelInput(ReadableByteChannel channel, Charset charset) {
		Reader channelReader = Channels.newReader(channel, charset.newDecoder(), -1);
		this.reader = new BufferedReader(channelReader); // add mark() and reset() support
	}
	
	@Override
	public Iterator<String> iterator() {
		return new SqlStatementsIterator(this);
	}

	/**
	 * Returns next parsed SQL statement or null if end of input was reached.
	 * 
	 * @throws IOException When underlining stream or channel throws IOException or when input unexpectedly
	 * end in the middle of comment or string.
	 */
	public String getNextStatement() throws IOException {
		if (!nextStatementAvailable()) {
			return null;
		}
		
		return getNextReadyStatement();
	}

	private String getNextReadyStatement() throws IOException {
		StringBuilder sb = new StringBuilder();
		boolean inStatement = false;
		for (;;) {
			if (matchComment(sb)) {
				continue;
			}
			else if (matchString(sb)) {
				continue;
			}
			else if (match(delimiter)) {
				break;
			}
			else {
				int read = reader.read();
				if (read == -1) {
					if (inStatement && requireLastDelimiter) {
						throw new IOException("Unexpected end of input");
					}
					else {
						logger.warn("Missing terminating semicolon in SQL statement");
						break;
					}
				}
				inStatement = !Character.isWhitespace((char) read);
				sb.append((char) read);
			}
		}
		
		return sb.toString();
	}

	private boolean nextStatementAvailable() throws IOException {
		for (;;) {
			if (matchSingleLineComment()) {
				continue;
			}
			reader.mark(1);
			int read = reader.read();
			if (read == -1) {
				return false;
			}
			else if (!Character.isWhitespace((char) read)) {
				reader.reset();
				return true;
			}
		}
	}
	
	private boolean matchComment(StringBuilder sb) throws IOException {
		if (match(MULTILINE_COMMENT_START)) {
			sb.append(MULTILINE_COMMENT_START);
			while (!match(MULTILINE_COMMENT_END)) {
				int read = reader.read();
				if (read == -1) {
					throw new IOException("Unexpected end of input while in multi-line comment");
				}
				else {
					sb.append((char) read);
				}
			}
			sb.append(MULTILINE_COMMENT_END);
			return true;
		}
		else if (matchSingleLineCommentStart()) {
			while (!matchEndOfLine()) {
				if (reader.read() == -1) {
					logger.warn("End of input after single-line SQL comment");
					return true;
				}
			}
			return true;
		}
		else {
			return false;
		}
	}
	
	private boolean matchSingleLineComment() throws IOException {
		if (matchSingleLineCommentStart()) {
			while (!matchEndOfLine()) {
				if (reader.read() == -1) {
					logger.warn("End of input after single-line SQL comment");
					return true;
				}
			}
			return true;
		}
		return false;
	}
	
	private boolean matchString(StringBuilder sb) throws IOException {
		if (matchQuote()) {
			sb.append(QUOTE_CHAR);
			for (;;) {
				if (backslashQuoteEscaping && match(BACKSLASH_ESCAPED_QUOTE)) {
					sb.append(BACKSLASH_ESCAPED_QUOTE);
				}
				else if (match(DOUBLE_QUOTE)) {
					sb.append(DOUBLE_QUOTE);
				}
				else {
					int read = reader.read();
					if (read == -1) {
						throw new IOException("Unexpected end of input while in string");
					}
					else {
						sb.append((char) read);
						if (read == QUOTE_CHAR) {
							break;
						}
					}
				}
			}
			return true;
		}
		return false;
	}
	
	private boolean matchSingleLineCommentStart() throws IOException {
		return match(SINGLELINE_COMMENT_START);
	}
	
	private boolean matchEndOfLine() throws IOException {
		return match("\r\n") || match("\n\r") || match("\n") || match("\r");
	}
	
	private boolean matchQuote() throws IOException {
		reader.mark(1);
		int read = reader.read();
		if (read == QUOTE_CHAR) {
			return true;
		}
		else {
			reader.reset();
			return false;
		}
	}
	
	private boolean match(String match) throws IOException {
		reader.mark(match.length());
		
		for (int i = 0; i < match.length(); i++) {
			int read = reader.read();
			if (read == -1) {
				reader.reset();
				return false;
			}
			char readChar = (char) read;
			char matchedChar = match.charAt(i);
			if (readChar != matchedChar) {
				reader.reset();
				return false;
			}
		}
		return true;
	}
	
	private final class SqlStatementsIterator implements Iterator<String> {
		
		private Reader reader; // only for iterator validity checking 

		public SqlStatementsIterator(SQLScriptParser parser) {
			this.reader = parser.reader;
		}
		
		@Override
		public boolean hasNext() {
			if (SQLScriptParser.this.reader != this.reader) {
				// somebody called setInput(..) and then used this old iterator
				throw new IllegalStateException("Invalid iterator");
			}
			
			try {
				return nextStatementAvailable();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String next() {
			try {
				return getNextReadyStatement();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove() not supported on this iterator");
		}
	}
	
}
