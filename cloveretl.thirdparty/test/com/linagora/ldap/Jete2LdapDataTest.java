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
package com.linagora.ldap;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.linagora.ldap.Jetel2LdapData.Jetel2LdapByte;
import com.linagora.ldap.Jetel2LdapData.Jetel2LdapString;

/**
 * @author Roland (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 2. 2017
 */
public class Jete2LdapDataTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCreateJetel2LdapByteInstance() {
		Jetel2LdapByte jetel2LdapByte = new Jetel2LdapByte();
		assertNull(jetel2LdapByte.getMultiSeparator());
	}
	
	@Test
	public void testCreateJetel2LdapStringInstanceNull() {
		Jetel2LdapString jetel2LdapString = new Jetel2LdapString(null);
		assertNull(jetel2LdapString.getMultiSeparator());
	}
	
	@Test
	public void testCreateJetel2LdapStringInstance() {
		Jetel2LdapString jetel2LdapString = new Jetel2LdapString("testSeparator");
		assertNotNull(jetel2LdapString.getMultiSeparator());
	}

}
