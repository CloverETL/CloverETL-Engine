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
package org.jetel.component.fileoperation.pool;

import java.net.URI;

import org.jetel.component.fileoperation.pool.DefaultAuthority.ProxyHelper;

import junit.framework.TestCase;

public class ProxyHelperTest extends TestCase {

	public void testProxyHelper() {
		test(
				"s3:(proxy://user:password@hostname.com:8080)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com/path/*.csv",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com/path/*.csv",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				null
		);

		test(
				"s3:(proxy://user:password@hostname.com:8080)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"s3:(proxysocks://user:password@hostname.com:8080)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxysocks://user:password@hostname.com:8080"
		);

		test(
				"s3:(proxy://hostname.com:8080)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxy://hostname.com:8080"
		);

		test(
				"s3:(proxysocks://hostname.com:8080)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxysocks://hostname.com:8080"
		);

		test(
				"s3:(proxy://hostname.com)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxy://hostname.com"
		);

		test(
				"s3:(proxysocks://hostname.com)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"proxysocks://hostname.com"
		);

		test(
				"s3:(direct:)//AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"s3://AKIAIG6EFMJBH6F7WYZQ:SECRETKEY@s3.eu-central-1.amazonaws.com",
				"direct:"
		);

		test(
				"http:(proxy://user:password@hostname.com:8080)//www.google.com",
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"http:(proxy://user:password@hostname.com:8080)//www.google.com",
				null,
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"http:(proxy://user:password@hostname.com:8080)//www.google.com",
				"",
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"http:(proxy://user:password@hostname.com:8080)//www.google.com",
				"direct:",
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080"
		);

		test(
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080",
				"http://www.google.com",
				"proxy://user:password@hostname.com:8080"
		);
	}

	private void test(String input, String proxyString, String expectedUri, String expectedProxyString) {
		URI inputUri = URI.create(input);
		ProxyHelper proxyHelper = (proxyString == null) ? ProxyHelper.getInstance(inputUri) : ProxyHelper.getInstance(inputUri, proxyString); // test both variants of the method
		assertEquals(URI.create(expectedUri), proxyHelper.uri);
		assertEquals(expectedProxyString, proxyHelper.proxyString);
	}

	private void test(String input, String expectedUri, String expectedProxyString) {
		test(input, null, expectedUri, expectedProxyString);
	}
	
}
