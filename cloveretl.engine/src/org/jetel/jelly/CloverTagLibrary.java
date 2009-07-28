
package org.jetel.jelly;

import org.apache.commons.jelly.TagLibrary;
import org.jetel.jelly.tags.MappingTag;

/**
 *
 * @author Pavel Pospichal
 */
public class CloverTagLibrary extends TagLibrary {

    public static final String CLOVER_NS_IN_JELLY = "clover:";
    
    public CloverTagLibrary() {
        registerTag("Mapping", MappingTag.class);
    }
}
