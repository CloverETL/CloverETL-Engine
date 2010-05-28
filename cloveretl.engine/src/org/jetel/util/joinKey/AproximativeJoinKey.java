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
package org.jetel.util.joinKey;

import org.jetel.data.Defaults;

public class AproximativeJoinKey{
	
	String master, slave;
	int maxDiffrence;
	double weight;
	boolean[] strength = new boolean[4];

	public AproximativeJoinKey(String master, String slave) {
		super();
		this.master = master.startsWith(Defaults.CLOVER_FIELD_INDICATOR) ? 
				master.substring(Defaults.CLOVER_FIELD_INDICATOR.length()) : master;
		if (slave != null) {
			this.slave = slave.startsWith(Defaults.CLOVER_FIELD_INDICATOR) ? 
					slave.substring(Defaults.CLOVER_FIELD_INDICATOR.length()) : slave;
		}
	}

	public int getMaxDiffrence() {
		return maxDiffrence;
	}

	public void setMaxDiffrence(int maxDiffrence) {
		this.maxDiffrence = maxDiffrence;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public boolean[] getStrength() {
		return strength;
	}
	
	public void setStrenght(int level, boolean value){
		strength[level] = value;
	}

	public String getMaster() {
		return master;
	}

	public String getSlave() {
		return slave;
	}
	
	@Override
	public String toString() {
		return Defaults.CLOVER_FIELD_INDICATOR + master + " = " + Defaults.CLOVER_FIELD_INDICATOR + slave + 
			'(' + maxDiffrence + ' ' + weight + ' ' + strength[0] + ' ' + strength[1] + ' ' + strength[2] +
			' ' + strength[3] + ')';
	}
}