package com.knime.bigdata.spark.jobserver.client.jar;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

/**
 * An exception thrown when trying to compile Java programs from strings
 * containing source.
 *
 * @author <a href="mailto:David.Biesack@sas.com">David J. Biesack</a>
 */
public class CharSequenceCompilerException extends Exception {
   private static final long serialVersionUID = 1L;
   /**
    * The fully qualified name of the class that was being compiled.
    */
   private Set<String> classNames;
   // Unfortunately, Diagnostic and Collector are not Serializable, so we can't
   // serialize the collector.
   transient private DiagnosticCollector<JavaFileObject> diagnostics;

   CharSequenceCompilerException(final String message,
         final Set<String> qualifiedClassNames, final Throwable cause,
         final DiagnosticCollector<JavaFileObject> aDiagnostics) {
      super(message, cause);
      setClassNames(qualifiedClassNames);
      setDiagnostics(aDiagnostics);
   }

   CharSequenceCompilerException(final String message,
         final Set<String> qualifiedClassNames,
         final DiagnosticCollector<JavaFileObject> aDiagnostics) {
      super(message);
      setClassNames(qualifiedClassNames);
      setDiagnostics(aDiagnostics);
   }

   CharSequenceCompilerException(final Set<String> qualifiedClassNames,
         final Throwable cause, final DiagnosticCollector<JavaFileObject> aDiagnostics) {
      super(cause);
      setClassNames(qualifiedClassNames);
      setDiagnostics(aDiagnostics);
   }

   private void setClassNames(final Set<String> qualifiedClassNames) {
      // create a new HashSet because the set passed in may not
      // be Serializable. For example, Map.keySet() returns a non-Serializable
      // set.
      classNames = new HashSet<String>(qualifiedClassNames);
   }

   private void setDiagnostics(final DiagnosticCollector<JavaFileObject> aDiagnostics) {
      this.diagnostics = aDiagnostics;
   }

   /**
    * Gets the diagnostics collected by this exception.
    *
    * @return this exception's diagnostics
    */
   public DiagnosticCollector<JavaFileObject> getDiagnostics() {
      return diagnostics;
   }

   /**
    * @return The name of the classes whose compilation caused the compile
    *         exception
    */
   public Collection<String> getClassNames() {
      return Collections.unmodifiableSet(classNames);
   }
}
