package org.jetel.graph;

import java.util.List;

import org.jetel.util.XmlCtlDataUtil;
import org.jetel.util.XmlCtlDataUtil.XmlData;

import junit.framework.TestCase;


/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class XmlCtlDataUtilTest extends TestCase{

	private final static String PREFIX = "![CTLDATA[";
	private final static String POSTFIX = "]]";
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}
	
	public void testParseCTLData() throws Exception {
		XmlCtlDataUtil.XmlData data;
		String v1 = "v1";
		String v2 = "v2";
		String v3 = "v3";
		String v4 = "v4";
		String v5 = "v5";
		String v6 = "v6";
		List<XmlData> xmlData = 
			XmlCtlDataUtil.parseCTLData(PREFIX + v2 + PREFIX + v3 + POSTFIX + v4 + POSTFIX + PREFIX + v5);
		data = xmlData.get(0);
		assertEquals(true, data.isCTLData() && data.getData().equals(v2 + PREFIX + v3));
		data = xmlData.get(1);
		assertEquals(true, !data.isCTLData() && data.getData().equals(v4 + POSTFIX + PREFIX + v5));
		
		xmlData = XmlCtlDataUtil.parseCTLData(v1 + POSTFIX + PREFIX + v2 + PREFIX + v3 + POSTFIX + PREFIX + POSTFIX + POSTFIX + v4 + POSTFIX + PREFIX + v5 + POSTFIX + v6);
		data = xmlData.get(0);
		assertEquals(true, !data.isCTLData() && data.getData().equals(v1 + POSTFIX));
		data = xmlData.get(1);
		assertEquals(true, data.isCTLData() && data.getData().equals(v2 + PREFIX + v3));
		data = xmlData.get(2);
		assertEquals(true, data.isCTLData() && data.getData().equals(""));
		data = xmlData.get(3);
		assertEquals(true, !data.isCTLData() && data.getData().equals(POSTFIX + v4 + POSTFIX));
		data = xmlData.get(4);
		assertEquals(true, data.isCTLData() && data.getData().equals(v5));
		data = xmlData.get(5);
		assertEquals(true, !data.isCTLData() && data.getData().equals(v6));
	}
	
}

