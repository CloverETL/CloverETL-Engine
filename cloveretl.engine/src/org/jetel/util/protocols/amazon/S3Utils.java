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
import java.io.IOException;

import org.jetel.util.ExceptionUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 10. 2015
 */
public class S3Utils {
	
	private S3Utils() {
		throw new UnsupportedOperationException();
	}
	
	public static void uploadFile(TransferManager tm, File file, String targetBucket, String targetKey) throws IOException {
		Upload upload = null;
		try {
			upload = tm.upload(targetBucket, targetKey, file);
			upload.waitForCompletion();
//			if (file.length() <= MULTIPART_UPLOAD_THRESHOLD) {
//				try {
//					CopyObjectRequest copyRequest = new CopyObjectRequest(targetBucket, targetKey, targetBucket, targetKey);
//					copyRequest.withNewObjectMetadata(new ObjectMetadata());
//					// unsafe, we don't check the result
//					// the connection pool might abort the operation before it completes
//					tm.copy(copyRequest); // copy object to itself to update ETag
//				} catch (Exception ex) {
//					log.warn("Metadata update failed: " + targetBucket + "/" + targetKey, ex);
//				}
//			}
		} catch (AmazonClientException e) {
			throw ExceptionUtils.getIOException(e);
		} catch (InterruptedException e) {
			if (upload != null) {
				upload.abort(); // this is necessary!
			}
			throw ExceptionUtils.getIOException(e);
		}
	}

	public static ListObjectsRequest listObjectRequest(String bucketName, String prefix, String delimiter) {
		return new ListObjectsRequest(bucketName, prefix, null, delimiter, Integer.MAX_VALUE);
	}
	
	public static IOException getIOException(Throwable t) {
		// special handling of AmazonServiceException is no longer needed
		return ExceptionUtils.getIOException(t);
	}
	
}
