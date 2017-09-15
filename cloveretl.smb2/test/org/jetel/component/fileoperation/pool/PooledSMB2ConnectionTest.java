package org.jetel.component.fileoperation.pool;

import java.lang.reflect.Method;
import java.net.URI;

import org.jetel.test.CloverTestCase;

public class PooledSMB2ConnectionTest extends CloverTestCase {

	public void testDomain() throws Exception {
		URI uri = new URI("smb2://mydomain%3BAdministrator:semafor4@virt-pink/smbtest/");
		Authority authority = SMB2Authority.newInstance(uri);
		try (PoolableConnection connection = ConnectionPool.getInstance().borrowObject(authority)) {
			assertEquals("mydomain", getDomain(connection));
		}
	}

	public static String getDomain(PoolableConnection connection) {
		try {
			Method method = connection.getClass().getDeclaredMethod("getDomain");
			method.setAccessible(true);
			return (String) method.invoke(connection);
		} catch (Exception e) {
			return null;
		}
	}
	
}
