/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.node.scripting.java.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.tools.JavaFileObject.Kind;

import org.eclipse.jdt.internal.compiler.tool.EclipseFileObject;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.util.FileUtil;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler.JavaVersion;

/**
 * Simple compiler to translate in-memory Java code into compiled byte code.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SuppressWarnings("restriction")
public class SourceCompiler {

    private final String m_className;

    private final String m_javaCode;

    private final Map<String, byte[]> m_bytecode;

    private final File[] m_classpath;

    private final JavaVersion m_javaVersion;

    /**
     * Creates a new CompiledModelPortObject from java code.
     *
     * @param aClassName The name of the class to compile
     * @param javaCode the code
     * @param classpath the class path entries
     * @param sparkVersion target Spark versions, used to select the target Java version
     * @throws CompilationFailedException when the code cannot be compiled
     * @throws ClassNotFoundException when the code has dependencies that cannot be resolved
     */
    public SourceCompiler(final String aClassName, final String javaCode, final File[] classpath,
        final SparkVersion sparkVersion) throws CompilationFailedException, ClassNotFoundException {

        m_className = aClassName;
        m_javaCode = javaCode;
        m_classpath = classpath;

        if (SparkVersion.V_2_2.compareTo(sparkVersion) <= 0) {
            m_javaVersion = JavaCodeCompiler.JavaVersion.JAVA_8;
        } else {
            m_javaVersion = JavaCodeCompiler.JavaVersion.JAVA_7;
        }

        m_bytecode = compile();
    }

    /**
     * @return the bytecode for the class and possibly inner classes
     */
    public Map<String, byte[]> getBytecode() {
        return m_bytecode;
    }

    private Map<String, byte[]> compile() throws CompilationFailedException {

        File tmpCompilationDirectory = null;
        try {
            tmpCompilationDirectory = FileUtil.createTempDir("SparkJavaSnippet");
            final File classDirectory = new File(tmpCompilationDirectory, "classes");
            if (!classDirectory.mkdir()) {
                throw new IOException("Failed to create temp directory for compiled classes: " + classDirectory.getAbsolutePath());
            }

            File codeFile = new File(tmpCompilationDirectory, m_className.concat(".java"));
            Files.write(codeFile.toPath(), Collections.singleton(m_javaCode));

            final EclipseFileObject snippetFile = new EclipseFileObject(m_className,
                codeFile.toURI(),
                Kind.SOURCE,
                StandardCharsets.UTF_8);

            final JavaCodeCompiler compiler = new JavaCodeCompiler(m_javaVersion, classDirectory);
            compiler.setSources(snippetFile);
            compiler.setClasspaths(m_classpath);
            compiler.compile();

            final File[] classes = classDirectory.listFiles(new FileFilter() {
                @Override
                public boolean accept(final File pathname) {
                    return pathname.isFile() && pathname.canRead() && pathname.getName().endsWith(".class");
                }
            });

            final Map<String, byte[]> bytecode = new HashMap<>();
            for (File clazz : classes) {
                final String classname = clazz.getAbsolutePath()
                        .substring(classDirectory.getAbsolutePath().length())
                        .replace(File.separator, ".")
                        .replaceFirst("\\.class$", "")
                        .replaceFirst("^\\.", "");

                bytecode.put(classname, Files.readAllBytes(clazz.toPath()));
            }
            return bytecode;

        } catch (IOException e) {
            throw new CompilationFailedException(
                "Failed to compile Java snippet", e);
        } finally {
            if (tmpCompilationDirectory != null) {
                if (tmpCompilationDirectory.exists()) {
                    FileUtil.deleteRecursively(tmpCompilationDirectory);
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
