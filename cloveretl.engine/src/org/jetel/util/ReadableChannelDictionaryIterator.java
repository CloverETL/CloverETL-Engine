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
package org.jetel.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.JetelException;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.util.file.FileUtils;


/***
 * Supports dictionary reading.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class ReadableChannelDictionaryIterator {

    private static Log defaultLogger = LogFactory.getLog(ReadableChannelDictionaryIterator.class);
	private static final String PARAM_DELIMITER = ":";
	private static final String DICT = "dict:";

	// charset and dictionary
	private String charset;
	private URL contextURL;
	private Dictionary dictionary;

	// dictionary value
	private Object value;
	
	// array of dictionary values
	private ObjectChannelIterator channelIterator;

	/**
	 * Constructor.
	 * @param dictionary
	 */
	public ReadableChannelDictionaryIterator(Dictionary dictionary) {
		this.dictionary = dictionary;
	}
	
	/**
	 * Sets dictionary url (file URL): dict:kye:processingType.
	 * @param inputDataSource
	 * @throws JetelException 
	 * @throws UnsupportedEncodingException 
	 */
	public void init(String inputDataSource) throws JetelException {
		// parse source
		String[] aSource = inputDataSource.substring(DICT.length()).split(PARAM_DELIMITER);
		String dictKey = aSource[0];
		ProcessingType dictProcesstingType = ProcessingType.fromString(aSource.length > 1 ? aSource[1] : null, ProcessingType.DISCRETE);
		if (dictionary == null) throw new RuntimeException("The component doesn't support dictionary reading.");
		value = dictionary.getValue(dictKey);
		if (value == null) throw new JetelException("Dictionary doesn't contain value for the key '" + dictKey + "'");
		
		// create channel iterator for one channel
		createChannelIterator(dictProcesstingType, dictKey);
	}

	/**
	 * Returns true if channel iterator has next value.
	 * @return
	 */
	public boolean hasNext() {
		return channelIterator != null && channelIterator.hasNext();
	}

	/**
	 * Gets next readable channel from dictionary.
	 * @return
	 */
	public ReadableByteChannel next() {
		return channelIterator.next();
	}
	
	/**
	 * Creates new channel iterator from dictionary value.
	 * @param dictProcesstingType
	 * @param dictKey
	 * @throws JetelException
	 * @throws UnsupportedEncodingException
	 */
	private void createChannelIterator(ProcessingType dictProcesstingType, String dictKey) throws JetelException {
		ReadableByteChannel rch;
		try {
			if (value instanceof List<?> || value instanceof Object[]) {
				channelIterator = ObjectChannelIterator.getInstance(value, charset, dictProcesstingType);
				if (channelIterator == null || !channelIterator.hasNext()) {
					defaultLogger.warn("Dictionary contains empty list for the key '" + dictKey + "'.");
					return;
				}
				return;
			} 
			else if (value instanceof InputStream) rch = Channels.newChannel((InputStream)value);
			else if (value instanceof ByteArrayOutputStream) rch = createReadableByteChannel(((ByteArrayOutputStream)value).toByteArray());
			else if (value instanceof ReadableByteChannel) rch = (ReadableByteChannel)value;
			else rch = createReadableByteChannel(value);
			
			if (dictProcesstingType == ProcessingType.SOURCE) {
				rch = createChannelFromSource(rch, charset);
			}
		} catch (UnsupportedEncodingException e) {
			throw new JetelException(e.getMessage(), e);
		}
		
		channelIterator = new DirectChannelIterator(rch);
	}
	
	/**
	 * @param readableByteChannel
	 * @param charset
	 * @return
	 * @throws JetelException
	 * @throws UnsupportedEncodingException
	 */
	private ReadableByteChannel createChannelFromSource(ReadableByteChannel readableByteChannel, String charset) throws JetelException, UnsupportedEncodingException {
		ByteBuffer dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		CharBuffer charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		CharsetDecoder decoder = Charset.forName(charset).newDecoder();
		dataBuffer.clear();
        dataBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		try {
			dataBuffer.compact();	// ready for writing
			readableByteChannel.read(dataBuffer);	// write to buffer
			dataBuffer.flip();	// ready reading
		} catch (IOException e) {
			throw new JetelException(e.getMessage());
		}
		charBuffer.compact();	// ready for writing
        decoder.decode(dataBuffer, charBuffer, false);
        charBuffer.flip();
        return createChannelFromFileName(charBuffer.toString());
	}
	
	/**
	 * Creates readable channel from String value of oValue.toString().
	 * 
	 * @param oValue
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	private ReadableByteChannel createReadableByteChannel(Object oValue) throws UnsupportedEncodingException {
		if (oValue == null) throw new NullPointerException("The field contains unsupported null value.");
		ByteArrayInputStream str = oValue instanceof byte[] ? 
				new ByteArrayInputStream((byte[])oValue) : new ByteArrayInputStream(oValue.toString().getBytes(charset));
		return Channels.newChannel(str);
	}

	/**
	 * Creates readable channel for a file name.
	 * 
	 * @param fileName
	 * @return
	 * @throws JetelException
	 */
	private ReadableByteChannel createChannelFromFileName(String fileName) throws JetelException {
		defaultLogger.debug("Opening input file " + fileName);
		try {
			ReadableByteChannel channel = FileUtils.getReadableChannel(contextURL, fileName);
			defaultLogger.debug("Reading input file " + fileName);
			return channel;
		} catch (IOException e) {
			throw new JetelException("File is unreachable: " + fileName, e);
		}
	}

	
	/**
	 * Iterators for dictionary values.
	 */
	private static abstract class ObjectChannelIterator implements Iterator<ReadableByteChannel> {
		protected int counter = 0;
		
		@SuppressWarnings("unchecked")
		public static ObjectChannelIterator getInstance(Object value, String charset, ProcessingType dictProcesstingType) {
			if (value instanceof CharSequence[]) {
				return new CharSeqValueArray((CharSequence[])value, charset);
			} else if (value instanceof List<?>) {
				List<?> list = (List<?>) value;
				if (list.size() == 0) return null;
				value = list.get(0);
				if (value instanceof CharSequence) {
					CharSequence[] aString = new CharSequence[list.size()];
					list.toArray(aString);
					return new CharSeqValueArray(aString, charset);
				} else if (value instanceof byte[]) {
					return new ByteValueArray((List<byte[]>)list);
				}
			}
			throw new RuntimeException("Cannot create input stream for class instance: '" + value.getClass() + "'.");
		}
	}
	
	private static class CharSeqValueArray extends ObjectChannelIterator {
		private CharSequence[] value;
		private String charset;

		public CharSeqValueArray(CharSequence[] value, String charset) {
			this.value = value;
			this.charset = charset;
		}
		
		@Override
		public boolean hasNext() {
			return counter < value.length;
		}
		@Override
		public ReadableByteChannel next() {
			try {
				return Channels.newChannel(new ByteArrayInputStream(value[counter++].toString().getBytes(charset)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		@Override
		public void remove() {
		}
	}
	
	private static class ByteValueArray extends ObjectChannelIterator {
		private List<byte[]> value;

		public ByteValueArray(List<byte[]> value) {
			this.value = value;
		}
		
		@Override
		public boolean hasNext() {
			return counter < value.size();
		}
		@Override
		public ReadableByteChannel next() {
			return Channels.newChannel(new ByteArrayInputStream(value.get(counter++)));
		}
		@Override
		public void remove() {
		}
	}

	private static class DirectChannelIterator extends ObjectChannelIterator {
		private ReadableByteChannel rch;
		private boolean hasNext;

		public DirectChannelIterator(ReadableByteChannel rch) {
			this.rch = rch;
			hasNext = true;
		}
		
		@Override
		public boolean hasNext() {
			return hasNext;
		}
		@Override
		public ReadableByteChannel next() {
			if (!hasNext()) return null;
			hasNext = false;
			return rch;
		}
		
		@Override
		public void remove() {
			hasNext = false;
		}
	}

	/**
	 * Sets a charset.
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}	
	
	public void setContextURL(URL contextURL) {
		this.contextURL = contextURL;
	}
	
}
