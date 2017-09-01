package org.jetel.component.fileoperation.pool;

import java.net.URI;

import org.jetel.test.CloverTestCase;

import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.share.DiskShare;

public class PooledSMB2ConnectionTest extends CloverTestCase {

	public void testDomain() throws Exception {
		URI uri = new URI("smb2://domain%3BAdministrator:semafor4@virt-pink/smbtest/");
		SMB2Authority authority = new SMB2Authority(uri);
		try (PooledSMB2Connection connection = (PooledSMB2Connection) ConnectionPool.getInstance().borrowObject(authority)) {
			DiskShare share = connection.getShare();
			AuthenticationContext authenticationContext = share.getTreeConnect().getSession().getAuthenticationContext();
			assertEquals("domain", authenticationContext.getDomain());
		}
	}

}
