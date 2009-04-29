package org.jetel.ctl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation for methods that should be recognized as CTL entry points
 * by the CTL-to-Java compiler.
 * 
 * @author mtomcanyi
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CTLEntryPoint {
	
	/**
	 * @return True when the method must be implemented in the CTL code (required by given transformation inerface)
	 */
	boolean required();
	
	/**
	 * @return Name of the corresponding CTL function
	 */
	String name();
	/**
	 * @return Parameter names - empty by default
	 */
	String[] parameterNames() default {};
}
