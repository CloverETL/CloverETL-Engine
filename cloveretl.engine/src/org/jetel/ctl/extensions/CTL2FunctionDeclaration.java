package org.jetel.ctl.extensions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation which provides additional details about CTL2 function. Should be used in connection with {@link TLFunctionLibraryExt}.
 * 
 * @author Branislav Repcek (branislav.repcek@javlin.eu)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CTL2FunctionDeclaration {

	/**
	 * @return Name of the function as used in CTL. If not set, name of the implementing Java method is used.
	 */
	String name() default "";
	
	/**
	 * @return class implementing the function in interpreted mode.
	 */
	Class< ? extends TLFunctionPrototype > impl();
}
