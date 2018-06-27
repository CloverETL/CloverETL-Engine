/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.graph.runtime;

import java.io.Serializable;

/**
 * @author Jiri (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 26, 2018
 */
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
