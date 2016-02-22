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
package org.jetel.util.protocols.amazon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.jetel.exception.TempFileCreationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

public class S3OutputStream extends OutputStream {
	
	// on creation: create outputstream writing to a file
	// remember url
	
	// on close and finalize:
	// check if not uploaded yet
	// if not, upload
	
	private static final long MULTIPART_UPLOAD_THRESHOLD = 5 * Constants.GB;
	
	private boolean uploaded;
	private File tempFile;
	private OutputStream os;
	private URL url;
	private TransferManager transferManager;
	
	public S3OutputStream(URL url) throws FileNotFoundException, IOException {
		this.uploaded = false;
		try {
			tempFile = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph()).newTempFile("cloveretl-amazons3-buffer", -1);
		} catch (TempFileCreationException e) {
			throw new IOException(e);
		}
		this.os = new FileOutputStream(tempFile);
		this.url = url;
	}
	
	private void upload() throws IOException {
		if (uploaded) {
			return;
		}
		try {
			uploaded = true;
			os.close();
			os = null;
			
			if (!S3InputStream.isS3File(url)) {
				throw new IllegalArgumentException("Not an Amazon S3 host");
			}
			
			String accessKey = S3InputStream.getAccessKey(url);
			String secretKey = S3InputStream.getSecretKey(url);
			String path = url.getFile();
			if (path.startsWith("/")) {
				path = path.substring(1);
			}
			
			AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
			AmazonS3Client service = new AmazonS3Client(credentials);
			transferManager = new TransferManager(service);
			TransferManagerConfiguration config = new TransferManagerConfiguration();
			config.setMultipartUploadThreshold(MULTIPART_UPLOAD_THRESHOLD);
			config.setMinimumUploadPartSize(MULTIPART_UPLOAD_THRESHOLD);
			transferManager.setConfiguration(config);
			
			String bucket = S3InputStream.getBucket(url);
	
			// CLO-4724:
			S3Utils.uploadFile(transferManager, tempFile, bucket, path);
			
		} finally {
			tempFile.delete();
			if (transferManager != null) {
				transferManager.shutdownNow();
			}
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}
	
	@Override
	public void close() throws IOException {
		upload();
	}
	
	@Override
	public void flush() throws IOException {
		os.flush();
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}

	@Override
	protected void finalize() throws Throwable {
		upload();
	}
}
