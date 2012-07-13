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
package org.jetel.util.file;

import static org.jetel.util.file.URLConnectionRequest.decodeString;
import static org.jetel.util.file.URLConnectionRequest.encode;
import junit.framework.TestCase;

public class URLConnectionRequestTest extends TestCase {

    private static final String BASIC_AUTH_HEADER_VALUE = "janko.hrasko%40gooddata.com:7897897883f22f8505cc36876be4f54c95ecf5";

    public void testLongBasicAuthHeaderBase64Encoding() {
        final String base64EncodedAuthHeader = encode(decodeString(BASIC_AUTH_HEADER_VALUE));
        assertNotNull(base64EncodedAuthHeader);
        assertFalse(base64EncodedAuthHeader.contains("\n"));
    }
}
