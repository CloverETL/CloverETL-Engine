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
package org.jetel.component.validator.params;

import org.jetel.test.CloverTestCase;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.5.2013
 */
public class LanguageSettingTest extends CloverTestCase {
	
	/**
	 * Tests that new instances are empty
	 */
	@Test
	public void testNewEmpty() {
		LanguageSetting temp = new LanguageSetting();
		assertEquals("", temp.getDateFormat().getValue());
		assertEquals("", temp.getNumberFormat().getValue());
		assertEquals("", temp.getLocale().getValue());
		assertEquals("", temp.getTimezone().getValue());
	}
	
	/**
	 * Tests that new instances are not empty
	 */
	@Test
	public void testNewNotEmpty() {
		LanguageSetting temp = new LanguageSetting(true);
		assertFalse(temp.getDateFormat().getValue().equals(""));
		assertFalse(temp.getNumberFormat().getValue().equals(""));
		assertFalse(temp.getLocale().getValue().equals(""));
		assertFalse(temp.getTimezone().getValue().equals(""));
	}
	
	/**
	 * Tests that hierarchic merge works (lower can override higher options)
	 */
	public void testHiearchicMerge() {
		LanguageSetting current;
		LanguageSetting parent;
		LanguageSetting merged;
		
		current = new LanguageSetting();
		parent = new LanguageSetting();
		parent.getDateFormat().setValue("DATEFORMAT");
		parent.getNumberFormat().setValue("NUMBERFORMAT");
		parent.getLocale().setValue("LOCALE");
		parent.getTimezone().setValue("TIMEZONE");
		
		merged = LanguageSetting.hierarchicMerge(current, parent);
		assertEquals("DATEFORMAT", merged.getDateFormat().getValue());
		assertEquals("NUMBERFORMAT", merged.getNumberFormat().getValue());
		assertEquals("LOCALE", merged.getLocale().getValue());
		assertEquals("TIMEZONE", merged.getTimezone().getValue());
		
		current = new LanguageSetting();
		parent = new LanguageSetting();
		parent.getDateFormat().setValue("DATEFORMAT");
		parent.getNumberFormat().setValue("NUMBERFORMAT");
		parent.getLocale().setValue("LOCALE");
		parent.getTimezone().setValue("TIMEZONE");
		current.getDateFormat().setValue("DATEFORMAT2");
		current.getNumberFormat().setValue("NUMBERFORMAT2");
		current.getLocale().setValue("LOCALE2");
		current.getTimezone().setValue("TIMEZONE2");
		
		merged = LanguageSetting.hierarchicMerge(current, parent);
		assertEquals("DATEFORMAT2", merged.getDateFormat().getValue());
		assertEquals("NUMBERFORMAT2", merged.getNumberFormat().getValue());
		assertEquals("LOCALE2", merged.getLocale().getValue());
		assertEquals("TIMEZONE2", merged.getTimezone().getValue());
	}
}
