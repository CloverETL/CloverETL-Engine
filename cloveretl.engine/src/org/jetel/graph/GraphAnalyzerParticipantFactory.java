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
package org.jetel.graph;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * @author adamekl (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 11. 2017
 */
public class GraphAnalyzerParticipantFactory {
	private static Log logger = LogFactory.getLog(GraphAnalyzerParticipantFactory.class);

	private final static Map<String, GraphAnalyzerParticipantDescription> analyzerMap = new HashMap<String, GraphAnalyzerParticipantDescription>();

	public static void init() {
		List<Extension> analyzerExtensions = Plugins.getExtensions(GraphAnalyzerParticipantDescription.EXTENSION_POINT_ID);
		for (Extension extension : analyzerExtensions) {
			try {
				GraphAnalyzerParticipantDescription desc = new GraphAnalyzerParticipantDescription(extension);
				desc.init();
				registerAnalyzer(desc);
			} catch (Exception e) {
				logger.error("Cannot create Custom Analyzer description, extension in plugin manifest is not valid.\n" + 
						"pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
			}
		}
	}

	public final static void registerAnalyzer(GraphAnalyzerParticipantDescription analyzers) {
		analyzerMap.put(analyzers.getType(), analyzers);
	}

	public final static List<GraphAnalyzerParticipant> getAllAnalyzerParticipants() {
		List<GraphAnalyzerParticipant> result = new ArrayList<>();
		for (String analyzerType : analyzerMap.keySet()) {
			result.add(createAnalyzer(analyzerType));
		}
		return result;
	}

	public final static GraphAnalyzerParticipant createAnalyzer(String analyzerType) {
		Class<? extends GraphAnalyzerParticipant> tClass = getAnalyzerClass(analyzerType);

		try {
			Constructor<? extends GraphAnalyzerParticipant> constructor = tClass.getConstructor();
			return constructor.newInstance();
		} catch (InvocationTargetException e) {
			throw new RuntimeException("Can't create Custom Analyzer of type " + analyzerType, e.getTargetException());
		} catch (Exception e) {
			throw new RuntimeException("Can't create Custom Analyzer of type " + analyzerType, e);
		}
	}

	public final static Class<? extends GraphAnalyzerParticipant> getAnalyzerClass(String analyzerType) {
		String className = null;
		GraphAnalyzerParticipantDescription analyzerDescription = analyzerMap.get(analyzerType);

		try {
			if (analyzerDescription == null) {
				className = analyzerType;
				return Class.forName(analyzerType).asSubclass(GraphAnalyzerParticipant.class);
			} else {
				className = analyzerDescription.getClassName();

				PluginDescriptor pluginDescriptor = analyzerDescription.getPluginDescriptor();

				return Class.forName(className, true, pluginDescriptor.getClassLoader()).asSubclass(GraphAnalyzerParticipant.class);
			}
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException("Unknown Custom Analyzer: " + analyzerType + " class: " + className, ex);
		} catch (Exception ex) {
			throw new RuntimeException("Unknown Custom Analyzer type: " + analyzerType, ex);
		}
	}
}