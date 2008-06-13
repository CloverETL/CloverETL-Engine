package org.jetel.util.string;

public class AproximativeJoinKey{
	
	String master, slave;
	int maxDiffrence;
	double weight;
	boolean[] strength = new boolean[4];

	public AproximativeJoinKey(String master, String slave) {
		super();
		this.master = master;
		this.slave = slave;
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
	
}