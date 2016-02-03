/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jan 28, 2016 by bjoern
 */
package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.InMemorySourceJavaFileObject;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import com.knime.bigdata.spark.SparkPlugin;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SourceCompiler {

    private final String m_className;

    private final String m_javaCode;

    private final Map<String, byte[]> m_bytecode;


    /**
     * Creates a new CompiledModelPortObject from java code.
     *
     * @param aClassName The name of the class to compile
     * @param javaCode the code
     * @throws CompilationFailedException when the code cannot be compiled
     * @throws ClassNotFoundException when the code has dependencies that cannot be resolved
     */
    public SourceCompiler(final String aClassName, final String javaCode)
        throws CompilationFailedException, ClassNotFoundException {

        this.m_className = aClassName;
        this.m_javaCode = javaCode;

        JavaCodeCompiler compiler = initCompiler();

        this.m_bytecode = compiler.getClassByteCode();
    }

    /**
     * @return the bytecode for the class and possibly inner classes
     */
    public Map<String, byte[]> getBytecode() {
        return m_bytecode;
    }


    private JavaCodeCompiler initCompiler() throws CompilationFailedException, ClassNotFoundException {
        final JavaCodeCompiler compiler = new JavaCodeCompiler(JavaCodeCompiler.JavaVersion.JAVA_7);
        final InMemorySourceJavaFileObject sourceContainer = new InMemorySourceJavaFileObject(m_className, m_javaCode);

        File[] classPathFiles = getClassPathFiles();
        compiler.setClasspaths(classPathFiles);
        compiler.setSources(sourceContainer);
        compiler.compile();

        return compiler;
    }

    /**
     * @return
     */
    private File[] getClassPathFiles() {
        final String[] orig = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        List<File> classPathFiles = new ArrayList<>(orig.length + 3);
        for (int i = 0; i < orig.length; i++) {
            classPathFiles.add(new File(orig[i]));
        }
        String root = SparkPlugin.getDefault().getPluginRootPath() + File.separator;
        addJars(classPathFiles, root + "resources");
        addJars(classPathFiles, root + "lib");
        return classPathFiles.toArray(new File[classPathFiles.size()]);
    }

    /**
     * @param aClassPathFiles
     * @param aRoot
     */
    private void addJars(final List<File> aClassPathFiles, final String aRoot) {
        File d = new File(aRoot + File.separator);
        final File[] files = d.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && f.getAbsolutePath().endsWith(".jar")) {
                    aClassPathFiles.add(f);
                }
            }
        }
    }


    /**
     * @return the name of the compiled class
     */
    public String getClassName() {
        return m_className;
    }
}
