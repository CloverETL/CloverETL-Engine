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
package org.jetel.component;

import java.util.List;

import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;

/**
 * {@link MetadataProvider} which allows to provide multiple metadata for a single port.
 * This feature is not completely supported now, but for example Designer
 * could use multiple metadata for generation metadata from template - user can choose
 * one of available metadata for dedicated port.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 10. 2013
 */
public interface MultiMetadataProvider extends MetadataProvider {

	public List<MVMetadata> getAllInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);

	public List<MVMetadata> getAllOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver);

}
