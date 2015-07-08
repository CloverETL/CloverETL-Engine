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
package org.jetel.component.tree.reader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import javax.xml.transform.sax.SAXSource;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20.3.2012
 */
public class ReadableByteChannelToSourceAdapter implements InputAdapter {

	private String charset;
	private XMLReader xmlReader;
	private URL contextUrl;

	public ReadableByteChannelToSourceAdapter(String charset) {
		this(charset, null);
	}
	
	public ReadableByteChannelToSourceAdapter(String charset, XMLReader xmlReader) {
		this(charset, xmlReader, null);
	}

	public ReadableByteChannelToSourceAdapter(String charset, XMLReader xmlReader, URL contextUrl) {
		this.charset = charset;
		this.xmlReader = xmlReader;
		this.contextUrl = contextUrl;
	}

	@Override
	public Object adapt(Object input) {
		InputSource inputSource;
		if (input instanceof URI) { // CLO-6632: URI is the preferred data source type for XML components
			try {
				String uri = input.toString();
				InputStream is = FileUtils.getInputStream(contextUrl, uri);
				inputSource = new InputSource(is);
				XmlUtils.setSystemId(inputSource, contextUrl, uri);
			} catch (IOException e) {
				throw new JetelRuntimeException("Could not read input " + input);
			}
		} else if (input instanceof ReadableByteChannel) {
			inputSource = new InputSource(Channels.newInputStream((ReadableByteChannel) input));
		} else {
			throw new JetelRuntimeException("Could not read input " + input);
		}

		if (charset != null) {
			inputSource.setEncoding(charset);
		}
		return new SAXSource(xmlReader, inputSource);
	}

}
