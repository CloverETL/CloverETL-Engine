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
package org.jetel.component.validator.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.jetel.component.validator.CustomRule;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.4.2013
 */
public class CustomRulesMapAdapter extends XmlAdapter<AdaptedCustomRule[], Map<Integer, CustomRule>> {

	@Override
	public AdaptedCustomRule[] marshal(Map<Integer, CustomRule> v) throws Exception {
		if(v == null) {
			return null;
		}
		AdaptedCustomRule[] output = new AdaptedCustomRule[v.size()];
		int i = 0;
		for(Entry<Integer, CustomRule> entry : v.entrySet()) {
			output[i++] = new AdaptedCustomRule(entry.getKey(), entry.getValue().getName(), entry.getValue().getCode());
		}
		return output;
	}

	@Override
	public Map<Integer, CustomRule> unmarshal(AdaptedCustomRule[] v) throws Exception {
		Map<Integer, CustomRule> output = new HashMap<Integer, CustomRule>();
		for(AdaptedCustomRule entry : v) {
			output.put(entry.getId(), new CustomRule(entry.getName(), entry.getCode()));
		}
		return output;
	}

}
