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
 * Created on 20.11.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import sun.nio.ByteBuffered;

import junit.framework.TestCase;

/**
 * @author david
 * @since  20.11.2006
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class DynamicRecordBufferTest extends TestCase {

    
    DynamicRecordBuffer buffer;
    ByteBuffer byteBuffer1,byteBuffer2;
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        Defaults.init();
        buffer=new DynamicRecordBuffer();
        buffer.init();
        byteBuffer1=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        byteBuffer2=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        int limit1=byteBuffer1.asCharBuffer().append("THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING").position();
                //" THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING *****").position();
        byteBuffer1.flip();
        byteBuffer1.limit(limit1);
        int limit2=byteBuffer2.asCharBuffer().append("THIS IS A TESTING DATA STRING *********************************************************** THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING *****").position();
        byteBuffer2.flip();
        byteBuffer2.limit(limit2);
    }

  
    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#writeRecord(java.nio.ByteBuffer)}.
     */
    public void testWriteRecord() throws IOException {
        // first, write 100 records
        for(int i=0;i<500;i++){
            buffer.writeRecord(byteBuffer1);
            byteBuffer1.rewind();
            buffer.writeRecord(byteBuffer2);
            byteBuffer2.rewind();
        }
        buffer.setEOF();
        System.out.println("has file:"+buffer.isHasFile());
        System.out.println("buffered records:"+buffer.getBufferedRecords());
        // first, read records
            byteBuffer1.clear();
            int i=0;
            while(buffer.readRecod(byteBuffer1)){
            System.out.println(++i+":"+byteBuffer1.asCharBuffer().toString());
            byteBuffer1.clear();
            }
        buffer.close();
    }

    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#readRecod(java.nio.ByteBuffer)}.
     */
    public void testReadRecod() throws IOException{
       
    }

    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#isEmpty()}.
     */
    public void testIsEmpty() {
    }

}
