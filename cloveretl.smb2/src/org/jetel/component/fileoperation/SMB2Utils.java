package org.jetel.component.fileoperation;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.Set;

import org.jetel.component.fileoperation.pool.PooledSMB2Connection;
import org.jetel.util.file.FileUtils;
import org.jetel.util.stream.DelegatingOutputStream;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.smbj.share.DiskShare;

public class SMB2Utils {

	public static String getPath(URI uri) {
		return new SMB2Path(uri).getPath();
	}
	
	public static String getPath(URL url) {
		return new SMB2Path(url).getPath();
	}
	
	private static com.hierynomus.smbj.share.File openFile(DiskShare share, String path, Set<AccessMask> accessMask, SMB2CreateDisposition createDisposition) throws IOException {
		return share.openFile(path, accessMask, null, null, createDisposition, null);
	}
	
	public static InputStream getInputStream(PooledSMB2Connection connection, URL source) throws IOException {
		return getInputStream(connection, getPath(source));
	}
	
	public static OutputStream getOutputStream(PooledSMB2Connection connection, URL target, boolean append) throws IOException {
		return getOutputStream(connection, getPath(target), append);
	}

	public static InputStream getInputStream(PooledSMB2Connection connection, URI source) throws IOException {
		return getInputStream(connection, getPath(source));
	}
	
	public static OutputStream getOutputStream(PooledSMB2Connection connection, URI target, boolean append) throws IOException {
		return getOutputStream(connection, getPath(target), append);
	}

	private static InputStream getInputStream(PooledSMB2Connection connection, String path) throws IOException {
		DiskShare share = connection.getShare();
		
		final PooledSMB2Connection finalConnection = connection;
		final com.hierynomus.smbj.share.File file = openFile(share, path, EnumSet.of(AccessMask.FILE_READ_DATA), SMB2CreateDisposition.FILE_OPEN);
		InputStream is = file.getInputStream();
		is = new FilterInputStream(is) {

			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					FileUtils.closeAll(file, finalConnection);
				}
			}
			
		};
		return is;
	}

	private static OutputStream getOutputStream(PooledSMB2Connection connection, String path, boolean append) throws IOException {
		DiskShare share = connection.getShare();
		
		final PooledSMB2Connection finalConnection = connection;
		final com.hierynomus.smbj.share.File file = openFile(share, path, EnumSet.of(AccessMask.FILE_WRITE_DATA), append ? SMB2CreateDisposition.FILE_OPEN_IF : SMB2CreateDisposition.FILE_SUPERSEDE);
		OutputStream os = file.getOutputStream();
		os = new DelegatingOutputStream(os) {

			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					FileUtils.closeAll(file, finalConnection);
				}
			}
			
		};
		return os;
	}

}
