/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 6.12.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.jetel.util.bytes.ByteBufferUtils;

public class TestByteBufferUtils extends TestCase {

    ByteBuffer buffer;
    
    protected void setUp() throws Exception {
        super.setUp();
        buffer=ByteBuffer.allocateDirect(512);
    }

    public void testEncodeLength() {
     for(int i=0;i<=10000000;i++){
         buffer.clear();
         ByteBufferUtils.encodeLength(buffer, i);
         buffer.flip();
         assertEquals(ByteBufferUtils.lengthEncoded(i),buffer.remaining());
         int read=ByteBufferUtils.decodeLength(buffer);
         assertEquals(i, read);
         if (i%1000000==0) System.out.print("+");
         buffer.clear();
         ByteBufferUtils.encodeLength(buffer, Integer.MAX_VALUE);
         buffer.flip();
         assertEquals(ByteBufferUtils.lengthEncoded(Integer.MAX_VALUE),buffer.remaining());
         read=ByteBufferUtils.decodeLength(buffer);
         assertEquals(Integer.MAX_VALUE, read);
         
     }
        
    }
    
    public void testlengthEncode(){
        assertEquals(1, ByteBufferUtils.lengthEncoded(0));    
        assertEquals(1, ByteBufferUtils.lengthEncoded(127));
        assertEquals(2, ByteBufferUtils.lengthEncoded(128));
        assertEquals(2, ByteBufferUtils.lengthEncoded(256));
        assertEquals(3, ByteBufferUtils.lengthEncoded(2097151));
        assertEquals(4, ByteBufferUtils.lengthEncoded(268435455));
    }

}
