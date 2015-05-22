// This is based on code written by:
//Copyright (c) 2007 by David J. Biesack, All Rights Reserved.
// Author: David J. Biesack David.Biesack@sas.com
// Created on Nov 4, 2007
// (http://www.ibm.com/developerworks/java/library/j-jcomp/)
// ... but heavily modified for KNIME purposes

package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

import org.apache.spark.SparkContext;
import org.knime.sparkClient.jobs.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.typesafe.config.Config;

/**
 *
 * fill a template with source code, compile the code and add it to a jar file or just return an instance of the
 * compiled class
 *
 * @author dwk, based on work by the above mentioned
 */
final public class SparkJobCompiler {

    private final static Logger LOGGER = Logger.getLogger(SparkJobCompiler.class.getName());

    // Create a CharSequenceCompiler instance which is used to compile
    // expressions into Java classes which are then used to create the XY plots.
    // The -target 1.5 options are simply an example of how to pass javac
    // compiler
    // options (the generated source in this example is Java 1.5 compatible.)
    private final CharSequenceCompiler<KnimeSparkJob> compiler = new CharSequenceCompiler<KnimeSparkJob>(getClass()
        .getClassLoader(), Arrays.asList(new String[]{"-target", "1.8"}));

    // for unique class names
    private int classNameSuffix = 0;

    // package name; a random number is appended
    private static final String PACKAGE_NAME = "org.knime.sparkClient.jobs";

    // for secure package name
    private static final Random random = new Random();

    // the Java source template
    private String template;

    /**
     * fill a template with source code, compile the code and add it to a jar file
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aAdditionalImports
     * @param validationCode
     * @param aExecutionCode
     * @param aHelperMethodsCode
     * @return canonical name of compiled class
     */
    public String addKnimeSparkJob2Jar(@Nonnull final String aSourceJarPath, @Nonnull final String aTargetJarPath,
        @Nonnull final String aAdditionalImports, @Nonnull final String validationCode,
        @Nonnull final String aExecutionCode, @Nonnull final String aHelperMethodsCode) {
        final String className = "kj" + (classNameSuffix++) + digits();
        final String qName = PACKAGE_NAME + '.' + className;
        try {
            // generate semi-secure unique package and class names
            // compile the generated Java source
            compileKnimeSparkJob(className, aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode);
            byte[] bytes = compiler.getClassByteCode(qName);
            JarPacker.add2Jar(aSourceJarPath, aTargetJarPath, qName, bytes);
        } catch (CharSequenceCompilerException e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        }
        return qName;
    }

    /**
     * fill a template with source code, compile the code and return an instance of the compiled class
     * @param aAdditionalImports
     * @param validationCode
     * @param aExecutionCode
     * @param aHelperMethodsCode
     * @return instance of compiled class
     */
    public KnimeSparkJob newKnimeSparkJob(@Nonnull final String aAdditionalImports,
        @Nonnull final String validationCode, @Nonnull final String aExecutionCode,
        @Nonnull final String aHelperMethodsCode) {
        try {
            // generate semi-secure unique package and class names
            final String className = "kj" + (classNameSuffix++) + digits();
            // compile the generated Java source
            Class<KnimeSparkJob> compiledCode =
                compileKnimeSparkJob(className, aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode);
            return compiledCode.newInstance();
        } catch (CharSequenceCompilerException e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
        } catch (InstantiationException e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        }
        return NULL_JOB;
    }

    private Class<KnimeSparkJob> compileKnimeSparkJob(final String aClassName,
        @Nonnull final String aAdditionalImports, @Nonnull final String validationCode,
        @Nonnull final String aExecutionCode, @Nonnull final String aHelperMethodsCode) throws IOException,
        ClassCastException, CharSequenceCompilerException {
        // generate semi-secure unique package and class names
        // generate the source class as String
        final String source =
            fillTemplate(PACKAGE_NAME, aClassName, aAdditionalImports, validationCode, aExecutionCode,
                aHelperMethodsCode);
        // compile the generated Java source
        final DiagnosticCollector<JavaFileObject> errs = new DiagnosticCollector<JavaFileObject>();
        Class<KnimeSparkJob> compiledCode =
            compiler.compile(PACKAGE_NAME + '.' + aClassName, source, errs, new Class<?>[]{KnimeSparkJob.class});
        log(errs);
        return compiledCode;
    }

    /**
     * @return random hex digits with a '_' prefix
     */
    private String digits() {
        return '_' + Long.toHexString(random.nextLong());
    }

    /**
     * Return the Java source, substituting the given package name, class name, and actual code snippets
     *
     * @param packageName a valid Java package name
     * @param className a valid Java class name
     * @param expression text for a double expression, using double x
     * @return source for the new class extending the KnimeSparkJob abstract class
     * @throws IOException
     */
    private String fillTemplate(final String packageName, final String className, final String aAdditionalImports,
        final String validationCode, final String executionCode, final String helperMethods) throws IOException {
        if (template == null) {
            template = readTemplate();
        }
        // simplest "template processor":
        String source =
            template.replace("$packageName", packageName).replace("$className", className)
                .replace("$additionalImports", aAdditionalImports).replace("$validationCode", validationCode)
                .replace("$executionCode", executionCode).replace("$helperMethods", helperMethods);
        return source;
    }

    /**
     * Read the KnimeSparkJob source template
     *
     * @return a source template
     * @throws IOException
     */
    private String readTemplate() throws IOException {
        InputStream is = SparkJobCompiler.class.getResourceAsStream("SparkJob.java.template");
        int size = is.available();
        byte bytes[] = new byte[size];
        if (size != is.read(bytes, 0, size)) {
            throw new IOException();
        }
        is.close();
        return new String(bytes, "US-ASCII");
    }

    /**
     * Log diagnostics to logger
     *
     * @param diagnostics iterable compiler diagnostics
     */
    private void log(final DiagnosticCollector<JavaFileObject> diagnostics) {
        final StringBuilder msgs = new StringBuilder();
        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            msgs.append(diagnostic.getMessage(null)).append("\n");
        }
        if (msgs.length() > 0) {
            LOGGER.log(Level.INFO, msgs.toString());
        }
    }

    /**
     * Null Object pattern to use when there are exceptions with the job code
     */
    private static final KnimeSparkJob NULL_JOB = new KnimeSparkJob() {

        @Override
        protected SparkJobValidation validateWithContext(final SparkContext aContext, final Config aConfig) {
            return ValidationResultConverter.invalid("job compilation failure!");
        }

        @Override
        protected Object runJobWithContext(final SparkContext aContext, final Config aConfig) {
            return "ERROR";
        }

    };

}
