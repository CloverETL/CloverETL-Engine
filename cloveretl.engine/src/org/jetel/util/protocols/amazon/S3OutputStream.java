package org.jetel.util.protocols.amazon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.security.NoSuchAlgorithmException;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class S3OutputStream extends OutputStream {

	// on creation: create outputstream writing to a file
	// remember url
	
	// on close and finalize:
	// check if not uploaded yet
	// if not, upload
	
	private boolean uploaded;
	private File tempFile;
	private OutputStream os;
	private URL url;
	
	public S3OutputStream(URL url) throws FileNotFoundException, IOException {
		this.uploaded = false;
		this.tempFile = File.createTempFile("cloveretl-amazons3-buffer", null);
		this.os = new FileOutputStream(tempFile);
		this.url = url;
	}
	
	private void upload() throws IOException {
		if (uploaded) {
			return;
		}
		
		uploaded = true;
		os.close();
		os = null;
		
		if (!S3InputStream.isS3File(url)) {
			throw new IllegalArgumentException("Not an Amazon S3 host");
		}
		
		String accessKey = S3InputStream.getAccessKey(url);
		String secretKey = S3InputStream.getSecretKey(url);
		String file = url.getFile();
		if (file.startsWith("/")) {
			file = file.substring(1);
		}
		
		AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);
		RestS3Service service;
		
		try {
			service = new RestS3Service(credentials);
		} catch (S3ServiceException e) {
			throw new IOException(e);
		}

		String bucket = S3InputStream.getBucket(url);
		S3Bucket s3bucket = new S3Bucket(bucket); 

		S3Object uploadObject;
		try {
			uploadObject = new S3Object(s3bucket, tempFile);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException(e);
		}
		uploadObject.setKey(file);
		
		try {
			service.putObject(bucket, uploadObject);
		} catch (S3ServiceException e) {
			throw new IOException(e);
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
