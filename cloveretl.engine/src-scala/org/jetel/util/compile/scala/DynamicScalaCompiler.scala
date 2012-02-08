/**
 *
 */
package org.jetel.util.compile.scala
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22 Dec 2011
 */
class DynamicScalaCompiler(parentClassLoader: ClassLoader) {
  
  val compilerCL = parentClassLoader;
  
  //TODO better logging (log4j) and javadoc
  def compile(sourceCode: String, className: String): Class[_] = {
    println("Compile time of '" + className + "' class");
    
    val settings = new Settings();
//    settings.usejavacp.value = true;
    settings.deprecation.value = true;
    settings.verbose.value = true;

    compilerCL.loadClass("org.jetel.component.DataRecordTransform");
    
    val compiler = new IMain(settings) {
      override def parentClassLoader(): ClassLoader = {
        return compilerCL;
      }
      override lazy val compilerClasspath = Nil; 
    };
//    val compiler = new IMain(settings);
    compiler.setContextClassLoader();

    val compilationResult = compiler.compileString(sourceCode);

    println("Compilation result: " + compilationResult);

    return compiler.classLoader.loadClass(className);
  }

}