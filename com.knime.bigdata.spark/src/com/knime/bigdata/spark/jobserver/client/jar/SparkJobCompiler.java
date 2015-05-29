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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

import org.apache.spark.SparkContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
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

    // the Java source templates
    private String sparkJobTemplate;
    private String transformationTemplate;

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
     * @throws GenericKnimeSparkException in case of some error
     */
    public KnimeSparkJob addKnimeSparkJob2Jar(@Nonnull final String aSourceJarPath, @Nonnull final String aTargetJarPath,
        @Nonnull final String aAdditionalImports, @Nonnull final String validationCode,
        @Nonnull final String aExecutionCode, @Nonnull final String aHelperMethodsCode) throws GenericKnimeSparkException {
        final String className = "kj" + (classNameSuffix++) + digits();
        final String qName = PACKAGE_NAME + '.' + className;
        try {
            // generate semi-secure unique package and class names
            // compile the generated Java source
            Class<KnimeSparkJob> c = compileKnimeSparkJob(className, aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode);
            byte[] bytes = compiler.getClassByteCode(qName);
            JarPacker.add2Jar(aSourceJarPath, aTargetJarPath, qName, bytes);
            return c.newInstance();
        } catch (CharSequenceCompilerException e) {
            log(e.getDiagnostics());
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * add pre-compiled job to a jar file
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aCanonicalClassPath
     * @throws GenericKnimeSparkException in case of some error
     */
    public void addKnimeSparkJob2Jar(@Nonnull final String aSourceJarPath, @Nonnull final String aTargetJarPath,
        @Nonnull final String aCanonicalClassPath) throws GenericKnimeSparkException {
        try {
            JarPacker.add2Jar(aSourceJarPath, aTargetJarPath, aCanonicalClassPath);
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
        }
    }

    /**
     * fill a template with source code, compile the code and add it to a jar file
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aAdditionalImports
     * @param aTransformationCode
     * @param aHelperMethodsCode
     * @return canonical name of compiled class
     * @throws GenericKnimeSparkException in case of some error
     * /
    public String addTransformationSparkJob2Jar(@Nonnull final String aSourceJarPath, @Nonnull final String aTargetJarPath,
        @Nonnull final String aAdditionalImports, @Nonnull final String aTransformationCode,
        @Nonnull final String aHelperMethodsCode) throws GenericKnimeSparkException {
        final String className = "kt" + (classNameSuffix++) + digits();
        final String qName = PACKAGE_NAME + '.' + className;
        try {
            // generate semi-secure unique package and class names
            // compile the generated Java source
            final String helperMethodsCode = fillTransformationTemplate(packageName, className, aAdditionalImports, aTransformationCode, aHelperMethods)
            compileKnimeSparkJob(className, aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode);
            byte[] bytes = compiler.getClassByteCode(qName);
            JarPacker.add2Jar(aSourceJarPath, aTargetJarPath, qName, bytes);
        } catch (CharSequenceCompilerException e) {
            log(e.getDiagnostics());
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        } catch (ClassNotFoundException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
        return qName;
    }
*/

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
            log(e.getDiagnostics());
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
                fillJobTemplate(PACKAGE_NAME, aClassName, aAdditionalImports, validationCode, aExecutionCode,
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
    private String fillJobTemplate(final String packageName, final String className, final String aAdditionalImports,
        final String validationCode, final String executionCode, final String helperMethods) throws IOException {
        if (sparkJobTemplate == null) {
            sparkJobTemplate = readTemplate("SparkJob.java.template");
        }
        Map<String, String> snippets = new HashMap<String, String>();
        snippets.put("$packageName", packageName);
        snippets.put("$className", className);
        snippets.put("$additionalImports", aAdditionalImports);
        snippets.put("$validationCode", validationCode);
        snippets.put("$executionCode", executionCode);
        snippets.put("$helperMethods", helperMethods);

        return fillTemplate(sparkJobTemplate, snippets);
    }

    private String fillTransformationTemplate(final String packageName, final String className, final String aAdditionalImports,
        final String aTransformationCode, final String aHelperMethods) throws IOException {
        if (transformationTemplate == null) {
            transformationTemplate = readTemplate("UserDefinedTransformation.java.template");
        }
        Map<String, String> snippets = new HashMap<String, String>();
        snippets.put("$packageName", packageName);
        snippets.put("$className", className);
        snippets.put("$additionalImports", aAdditionalImports);
        snippets.put("$transformationCode", aTransformationCode);
        snippets.put("$helperMethods", aHelperMethods);

        return fillTemplate(transformationTemplate, snippets);
    }

    private static String fillTemplate(final String aTemplate, final Map<String, String> aSnippets) throws IOException {
        String source =  aTemplate;
        for (Map.Entry<String, String> entry : aSnippets.entrySet()) {
            source = source.replace(entry.getKey(), entry.getValue());
        }
        return source;
    }

    /**
     * Read the KnimeSparkJob or Transformation source template
     * @param name template name
     * @return a source template
     * @throws IOException
     */
    private String readTemplate(final String aName) throws IOException {
        InputStream is = SparkJobCompiler.class.getResourceAsStream(aName);
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
        public SparkJobValidation validate(final Config aConfig) {
            return ValidationResultConverter.invalid("job compilation failure!");
        }

        @Override
        protected JobResult runJobWithContext(final SparkContext aContext, final Config aConfig) {
            return JobResult.emptyJobResult().withMessage("ERROR");
        }

    };

}
