/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) (c) Javlin, a.s. (www.javlin.eu) (www.opensys.com)
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
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.util.file.key;

import org.jetel.exception.JetelException;
import org.jetel.test.CloverTestCase;
import org.jetel.util.key.KeyTokenizer;
import org.jetel.util.key.RecordKeyTokens;
import org.jetel.util.key.OrderType;
import org.jetel.util.string.Compare;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 7.10.2009
 */
public class KeyDescUtilsTest extends CloverTestCase {

    @Override
	protected void setUp() throws Exception {
        super.setUp();
        initEngine();
    }

    public void testParseKeyRecord() throws JetelException {
    	String rawKeyRecord = "field1(A);field2(d);field3(I);field4(r)";
    	
    	RecordKeyTokens keyRecord = KeyTokenizer.tokenizeRecordKey(rawKeyRecord);

    	assertTrue(rawKeyRecord.equalsIgnoreCase(keyRecord.toString()));
    	assertEquals(keyRecord.getKeyField(0).getFieldName(), "field1");
    	assertEquals(keyRecord.getKeyField(1).getFieldName(), "field2");
    	assertEquals(keyRecord.getKeyField(2).getFieldName(), "field3");
    	assertEquals(keyRecord.getKeyField(3).getFieldName(), "field4");

    	assertEquals(keyRecord.getKeyField(0).getOrderType(), OrderType.ASCENDING);
    	assertEquals(keyRecord.getKeyField(1).getOrderType(), OrderType.DESCENDING);
    	assertEquals(keyRecord.getKeyField(2).getOrderType(), OrderType.IGNORE);
    	assertEquals(keyRecord.getKeyField(3).getOrderType(), OrderType.AUTO);

    }
    
    public void testLegacyKeyOrderingDetection() throws JetelException {
    	String rawKeyRecord="field1;field2;field3";
    	
    	RecordKeyTokens keyRecord = KeyTokenizer.tokenizeRecordKey(rawKeyRecord);
    	
    	assertTrue(rawKeyRecord.equalsIgnoreCase(keyRecord.toString()));
    	assertEquals(keyRecord.getKeyField(0).getFieldName(), "field1");
    	assertEquals(keyRecord.getKeyField(1).getFieldName(), "field2");
    	assertEquals(keyRecord.getKeyField(2).getFieldName(), "field3");
    	
    	assertEquals(keyRecord.getKeyField(0).getOrderType(), null);
    	assertEquals(keyRecord.getKeyField(1).getOrderType(), null);
    	assertEquals(keyRecord.getKeyField(2).getOrderType(), null);
    }

}
