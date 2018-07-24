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
package org.jetel.graph.runtime;

import java.io.Serializable;

public class RestJobOutputData implements Serializable {

	private static final long serialVersionUID = 8429131100028818807L;
	
	private String contextUrl;
	private String fileUrl;
	private String contentType;
	private String encoding;
	private boolean attachment;
	
	public RestJobOutputData(String contextUrl, String fileUrl, String contentType, String encoding, boolean attachment) {
		this.contextUrl = contextUrl; 
		this.fileUrl = fileUrl;
		this.contentType = contentType;
		this.encoding = encoding;
		this.attachment = attachment;
	}

	public String getContextUrl() {
		return contextUrl;
	}

	public String getFileUrl() {
		return fileUrl;
	}

	public String getContentType() {
		return contentType;
	}

	public String getEncoding() {
		return encoding;
	}

	public boolean isAttachment() {
		return attachment;
	}
}
