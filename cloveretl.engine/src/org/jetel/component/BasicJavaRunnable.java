package org.jetel.component;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;

public abstract class BasicJavaRunnable implements JavaRunnable {

	protected TransformationGraph graph;
	protected Properties parameters;

	public BasicJavaRunnable() {
		this(null);
	}
	
	public BasicJavaRunnable(TransformationGraph graph) {
		this.graph = graph;
	}
	
	public TransformationGraph getGraph() {
		return graph;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;

	}

	public boolean init(Properties parameters)
			throws ComponentNotReadyException {
		
		this.parameters=parameters;
		return init();
	}
	
	public boolean init() {
		return true;
	}

	abstract public void run();
	
}
