package org.jetel.ctl.extensions;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;

import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.exception.JetelRuntimeException;

/**
 * <p>Helper class used by the CTL2 function libraries for discovering the CTL functions in the library. This class is
 * intended to be used as ancestor for the library class instead of plain {@link TLFunctionLibrary}. Using this class
 * can save a lot of work to library author as there is no need to write code to manually return instances of function
 * implementation classes.</p>
 * 
 * <p>Note that for this class to work properly one additional annotation has to be used on each CTL function -
 * {@link CTL2FunctionDeclaration}.</p>
 * <p>This helper properly handles static as well as inner classes if they are used for implementation of CTL method
 * in interpreted mode.</p>
 * 
 * @author Branislav Repcek (branislav.repcek@javlin.eu)
 */
public abstract class TLFunctionLibraryExt extends TLFunctionLibrary {
	
	private HashMap< String, Class< ? extends TLFunctionPrototype > > libraryFunctions;
	private String libraryName;
	
	public TLFunctionLibraryExt(String libraryName) {
		// TODO replace with proper logging
		// System.out.println("Initializing CTL2 library \"" + libraryName +"\" in " + getClass().getCanonicalName());
		
		this.libraryName = libraryName;
		libraryFunctions = findFunctions(this.getClass());
	}
	
	@Override
	public TLFunctionPrototype getExecutable(String functionName) throws IllegalArgumentException {

		Class< ? extends TLFunctionPrototype > prototypeClass = libraryFunctions.get(functionName);
		if (prototypeClass == null) {
			throw new TransformLangExecutorRuntimeException("Function '" + functionName + "' is not properly registered in the library '" +
					getName() + "'. Missing annotations?");
		}
		
		try {
			// On static classes we can use simple way of instantiation.
			if (Modifier.isStatic(prototypeClass.getModifiers())) {
				return prototypeClass.newInstance();
			} else {
				// Non-static inner classes.
				return prototypeClass.getConstructor(this.getClass()).newInstance(this);
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to instantiate implementation class for '" + functionName + "'.", e);
		}
	}
	
	@Override
	public String getName() {
		return libraryName;
	}
	
	/**
	 * <p>Find all functions in the library and for each function find the class which implements its wrapper for interpreted
	 * mode in CTL2.</p>
	 * <p>Note that functions have to be properly annotated with {@link CTL2FunctionDeclaration}. Also note that this
	 * function assumes that overloaded functions use one wrapper class for all overloads.</p>
	 * 
	 * @param library library class to search through.
	 * 
	 * @return map which maps function name to {@link CTL2FunctionWrapper} instance holding details about the
	 *         implementation of the function for interpreted as well as compiled mode.
	 * 
	 * @throws JetelRuntimeException if overloaded functions use more than one wrapper class.
	 */
	private static HashMap< String, Class< ? extends TLFunctionPrototype > > findFunctions(Class< ? extends TLFunctionLibrary > library)
	throws JetelRuntimeException {
		
		HashMap< String, Class< ? extends TLFunctionPrototype > > libFunctions = new HashMap< String, Class< ? extends TLFunctionPrototype > >();
		
		for (Method method: library.getMethods()) {
			CTL2FunctionDeclaration fnDecl = method.getAnnotation(CTL2FunctionDeclaration.class);
			
			if (fnDecl == null) {
				continue;
			}
			
			String name = fnDecl.name();
			if (name.equals("")) {
				// If name is not set, use the same name as the Java method name
				name = method.getName();
			}
			
			// TODO replace with proper logging on debug or trace level
			//System.out.println("\tRegistering function: \"" + name +"\" -> [" + method.getName() + ", " + fnDecl.impl().getCanonicalName() + "]");
			libFunctions.put(name, fnDecl.impl());
		}
		
		return libFunctions;
	}
}
