// This is based on code written by:
//Copyright (c) 2007 by David J. Biesack, All Rights Reserved.
// Author: David J. Biesack David.Biesack@sas.com
// Created on Nov 4, 2007
// (http://www.ibm.com/developerworks/java/library/j-jcomp/)
// ... but heavily modified for KNIME purposes

package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.InMemorySourceJavaFileObject;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;

/**
 *
 * fill a template with source code, compile the code and add it to a jar file or just return an instance of the
 * compiled class
 *
 * @author dwk, based on work by the above mentioned
 */
final public class SparkJobCompiler {

    private final static Logger LOGGER = Logger.getLogger(SparkJobCompiler.class.getName());

    // for unique class names
    private int classNameSuffix = 0;

    // package name; a random number is appended
   // private static final String PACKAGE_NAME = "com.knime.bigdata.spark.jobserver.jobs";

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
    public KnimeSparkJob addKnimeSparkJob2Jar(@Nonnull final String aSourceJarPath,
        @Nonnull final String aTargetJarPath, @Nonnull final String aAdditionalImports,
        @Nonnull final String validationCode, @Nonnull final String aExecutionCode,
        @Nonnull final String aHelperMethodsCode) throws GenericKnimeSparkException {
        SourceCompiler compiledJob =
            compileKnimeSparkJob(aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode);
        add2Jar(aSourceJarPath, aTargetJarPath, compiledJob.getClass().getCanonicalName(), compiledJob.getBytecode());
        return compiledJob.getInstance();
    }

    /**
     * add pre-compiled job to a jar file
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aCanonicalClassPath
     * @throws GenericKnimeSparkException in case of some error
     */
    public void addPrecompiledKnimeSparkJob2Jar(@Nonnull final String aSourceJarPath,
        @Nonnull final String aTargetJarPath, @Nonnull final String aCanonicalClassPath)
        throws GenericKnimeSparkException {
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
     */
    public KnimeSparkJob addTransformationSparkJob2Jar(@Nonnull final String aSourceJarPath,
        @Nonnull final String aTargetJarPath, @Nonnull final String aAdditionalImports,
        @Nonnull final String aTransformationCode, @Nonnull final String aHelperMethodsCode)
        throws GenericKnimeSparkException {
        final String className = "kt" + (classNameSuffix++) + digits();
        // generate semi-secure unique package and class names
        // compile the generated Java source
        final String source =
            fillTransformationTemplate(className, aAdditionalImports, aTransformationCode,
                aHelperMethodsCode);
        SourceCompiler compiledJob = compileAndCreateInstance(className, source);
        add2Jar(aSourceJarPath, aTargetJarPath, className, compiledJob.getBytecode());
        return compiledJob.getInstance();
    }

    /**
     * add the class with the given name to the jar, must be called after compileAndCreateInstance was called to compile
     * the class !
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param qName
     * @throws GenericKnimeSparkException
     */
    private void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String qName,
        final byte[] aBytes) throws GenericKnimeSparkException {
        try {
            JarPacker.add2Jar(aSourceJarPath, aTargetJarPath, qName, aBytes);
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * compile the given source under the given class name
     *
     * @param aClassName
     * @param aSource
     * @return new instance of compile class
     * @throws GenericKnimeSparkException
     */
    private SourceCompiler compileAndCreateInstance(final String aClassName, final String aSource)
        throws GenericKnimeSparkException {
        try {
            return new SourceCompiler(aClassName, aSource);
        } catch (ClassNotFoundException | CompilationFailedException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * fill a template with source code, compile the code and return an instance of the compiled class
     *
     * @param aAdditionalImports
     * @param validationCode
     * @param aExecutionCode
     * @param aHelperMethodsCode
     * @return instance of compiled class
     * @throws GenericKnimeSparkException
     */
    private SourceCompiler compileKnimeSparkJob(@Nonnull final String aAdditionalImports,
        @Nonnull final String validationCode, @Nonnull final String aExecutionCode,
        @Nonnull final String aHelperMethodsCode) throws GenericKnimeSparkException {

        final String className = "kj" + (classNameSuffix++) + digits();
        // generate semi-secure unique package and class names
        // compile the generated Java source
        final String source =
            fillJobTemplate(className, aAdditionalImports, validationCode, aExecutionCode,
                aHelperMethodsCode);
        return compileAndCreateInstance(className, source);
    }

    /**
     * fill a template with source code, compile the code and return an instance of the compiled class
     *
     * @param aAdditionalImports
     * @param validationCode
     * @param aExecutionCode
     * @param aHelperMethodsCode
     * @return instance of compiled class
     * @throws GenericKnimeSparkException
     */
    public KnimeSparkJob newKnimeSparkJob(@Nonnull final String aAdditionalImports,
        @Nonnull final String validationCode, @Nonnull final String aExecutionCode,
        @Nonnull final String aHelperMethodsCode) throws GenericKnimeSparkException {

        return compileKnimeSparkJob(aAdditionalImports, validationCode, aExecutionCode, aHelperMethodsCode)
            .getInstance();
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
     * @throws GenericKnimeSparkException
     */
    private String fillJobTemplate(final String className, final String aAdditionalImports,
        final String validationCode, final String executionCode, final String helperMethods)
        throws GenericKnimeSparkException {
        try {
            if (sparkJobTemplate == null) {
                sparkJobTemplate = readTemplate("SparkJob.java.template");
            }
            Map<String, String> snippets = new HashMap<String, String>();
            snippets.put("$packageName", "");
            snippets.put("$className", className);
            snippets.put("$additionalImports", aAdditionalImports);
            snippets.put("$validationCode", validationCode);
            snippets.put("$executionCode", executionCode);
            snippets.put("$helperMethods", helperMethods);

            return fillTemplate(sparkJobTemplate, snippets);
        } catch (IOException e) {
            throw new GenericKnimeSparkException(e);
        }

    }

    private String fillTransformationTemplate(final String className,
        final String aAdditionalImports, final String aTransformationCode, final String aHelperMethods)
        throws GenericKnimeSparkException {
        try {
            if (transformationTemplate == null) {
                transformationTemplate = readTemplate("UserDefinedTransformation.java.template");
            }
            Map<String, String> snippets = new HashMap<String, String>();
            //package $packageName;
            snippets.put("$packageName", "");
            snippets.put("$className", className);
            snippets.put("$additionalImports", aAdditionalImports);
            snippets.put("$transformationCode", aTransformationCode);
            snippets.put("$helperMethods", aHelperMethods);

            return fillTemplate(transformationTemplate, snippets);
        } catch (IOException e) {
            throw new GenericKnimeSparkException(e);
        }
    }

    private static String fillTemplate(final String aTemplate, final Map<String, String> aSnippets) throws IOException {
        String source = aTemplate;
        for (Map.Entry<String, String> entry : aSnippets.entrySet()) {
            source = source.replace(entry.getKey(), entry.getValue());
        }
        return source;
    }

    /**
     * Read the KnimeSparkJob or Transformation source template
     *
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

    private static class SourceCompiler {

        private byte[] m_bytecode;

        /**
         * Creates a new CompiledModelPortObject from java code.
         *
         * @param javaCode the code
         * @throws CompilationFailedException when the code cannot be compiled
         * @throws ClassNotFoundException when the code has dependencies that cannot be resolved
         */
        SourceCompiler(final String aClassName, final String javaCode) throws CompilationFailedException,
            ClassNotFoundException {
            setCode(aClassName, javaCode);
        }

        private String m_javaCode;

        private KnimeSparkJob m_jobInstance;

        /**
         * @return the bytecode
         */
        byte[] getBytecode() {
            return m_bytecode;
        }

        /**
         * Sets the java code for this port object.
         *
         * @param code the code
         * @throws CompilationFailedException when the code cannot be compiled
         * @throws ClassNotFoundException when the code has dependencies that cannot be resolved
         */
        private void setCode(final String aClassName, final String code) throws ClassNotFoundException,
            CompilationFailedException {
            m_javaCode = code;
            compileCode(aClassName);
        }

        private void compileCode(final String aClassName) throws CompilationFailedException, ClassNotFoundException {
            final JavaCodeCompiler compiler = new JavaCodeCompiler();

            final InMemorySourceJavaFileObject sourceContainer = new InMemorySourceJavaFileObject(aClassName, m_javaCode);

            String[] orig = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
            File[] classPathFiles = new File[orig.length + 4];
            for (int i=0; i<orig.length; i++) {
                classPathFiles[i] = new File(orig[i]);
            }
            final String root = SparkPlugin.getDefault().getPluginRootPath();
            classPathFiles[orig.length+1] = new File(root+"/bin/");
            classPathFiles[orig.length+2] = new File(root+"/lib/knimeSparkScalaClient.jar");
            classPathFiles[orig.length+3] = new File(root+"/lib/spark-assembly-1.2.1-hadoop2.4.0.jar");
            classPathFiles[orig.length] = new File(root+"/lib/job-server-api_2.10-0.5.1-SNAPSHOT.jar");
            compiler.setClasspaths(classPathFiles);

            compiler.setSources(sourceContainer);


            compiler.compile();

            final ClassLoader cl = compiler.createClassLoader(this.getClass().getClassLoader());

            //final String className = (PACKAGE_NAME+ "/" + aClassName).replaceAll("\\.", "/");
            final Class<KnimeSparkJob> jobClass = (Class<KnimeSparkJob>)cl.loadClass(aClassName);
            m_bytecode =  compiler.getClassByteCode().get(aClassName);
            try {
                m_jobInstance = jobClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new CompilationFailedException("Failed to create instance: ", e);
            }
        }

        /**
         * @return the compiled class.
         */
        KnimeSparkJob getInstance() {
            return m_jobInstance;
        }

    }

}
