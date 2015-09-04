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
package org.jetel.plugin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8. 7. 2015
 */
public class PluginDescriptorSorterTest extends CloverTestCase {

	public void test1() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 0);
	}

	public void test2() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 1);
		assertTrue(pluginDescriptors.get("a1") != null);
	}

	public void test3() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 3);
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(0).getId().equals("a1"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(1).getId().equals("a2"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(2).getId().equals("a3"));
	}

	public void test4() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1", "a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 3);
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(0).getId().equals("a2"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(1).getId().equals("a1"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(2).getId().equals("a3"));
	}

	public void test5() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1", "a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 3);
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(0).getId().equals("a2"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(1).getId().equals("a3"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(2).getId().equals("a1"));
	}

	public void test6() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1", "a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3", "a2"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 3);
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(0).getId().equals("a2"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(1).getId().equals("a3"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(2).getId().equals("a1"));
	}

	public void test7() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1", "a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3"));
		
		PluginDescriptorSorter.sort(pluginDescriptors);
		
		assertTrue(pluginDescriptors.size() == 3);
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(0).getId().equals("a3"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(1).getId().equals("a2"));
		assertTrue(new ArrayList<PluginDescriptor>(pluginDescriptors.values()).get(2).getId().equals("a1"));
	}

	public void test8() {
		LinkedHashMap<String, PluginDescriptor> pluginDescriptors = new LinkedHashMap<String, PluginDescriptor>();
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a1", "a2"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a3", "a1"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a4", "a1"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a5", "a1", "a2", "a3"));
		putPluginDescriptor(pluginDescriptors, createPluginDescriptor("a6"));
		
		try {
			PluginDescriptorSorter.sort(pluginDescriptors);
			assertTrue(false);
		} catch(Exception e) {
			//OK
		}
	}

	private void putPluginDescriptor(Map<String, PluginDescriptor> pluginDescriptors, PluginDescriptor pluginDescriptor) {
		pluginDescriptors.put(pluginDescriptor.getId(), pluginDescriptor);
	}
	
	private PluginDescriptor createPluginDescriptor(String id, String... prerequisities) {
		PluginDescriptor pluginDescriptor = new PluginDescriptor(null, null);
		pluginDescriptor.setId(id);
		for (String prerequisity : prerequisities) {
			pluginDescriptor.addPrerequisites(prerequisity, null, null);
		}
		return pluginDescriptor;
	}
	
}
