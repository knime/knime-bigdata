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
package com.knime.bigdata.spark.node.scripting.java.util;

import java.io.File;
import java.util.Map;

import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.InMemorySourceJavaFileObject;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

/**
 * Simple compiler to translate in-memory Java code into compiled byte code.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SourceCompiler {

    private final String m_className;

    private final String m_javaCode;

    private final Map<String, byte[]> m_bytecode;

    private final File[] m_classpath;

    /**
     * Creates a new CompiledModelPortObject from java code.
     *
     * @param aClassName The name of the class to compile
     * @param javaCode the code
     * @param classpath the class path entries
     * @throws CompilationFailedException when the code cannot be compiled
     * @throws ClassNotFoundException when the code has dependencies that cannot be resolved
     */
    public SourceCompiler(final String aClassName, final String javaCode, final File[] classpath)
        throws CompilationFailedException, ClassNotFoundException {

        this.m_className = aClassName;
        this.m_javaCode = javaCode;
        this.m_classpath = classpath;

        JavaCodeCompiler compiler = initCompiler();

        this.m_bytecode = compiler.getClassByteCode();
    }

    /**
     * @return the bytecode for the class and possibly inner classes
     */
    public Map<String, byte[]> getBytecode() {
        return m_bytecode;
    }

    private JavaCodeCompiler initCompiler() throws CompilationFailedException {
        final JavaCodeCompiler compiler = new JavaCodeCompiler(JavaCodeCompiler.JavaVersion.JAVA_7);
        final InMemorySourceJavaFileObject sourceContainer = new InMemorySourceJavaFileObject(m_className, m_javaCode);

        compiler.setClasspaths(m_classpath);
        compiler.setSources(sourceContainer);
        compiler.compile();

        return compiler;
    }

    /**
     * @return the name of the compiled class
     */
    public String getClassName() {
        return m_className;
    }
}
