/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   01.12.2011 (hofer): created
 */
package com.knime.bigdata.spark.node.scripting.util;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;

import org.eclipse.jdt.internal.compiler.tool.EclipseCompiler;

/**
 * Utility class to compile a java snippet.
 *
 * @author Heiko Hofer
 */
@SuppressWarnings("restriction")
public class SparkJavaSnippetCompiler {
//    private static final NodeLogger LOGGER =
//        NodeLogger.getLogger(JavaCodeCompiler.class);

    private SparkJavaSnippet m_snippet;
    private ArrayList<String> m_compileArgs;

    private EclipseCompiler m_compiler;

    /**
     * Create a new instance.
     *
     * @param snippet the snippet to compile.
     */
    public SparkJavaSnippetCompiler(final SparkJavaSnippet snippet) {
        m_snippet = snippet;
    }

    /**
     * Creates a compilation task.
     *
     * @param out a Writer for additional output from the compiler;
     * use System.err if null
     * @param digsCollector a diagnostic listener; if null use the compiler's
     * default method for reporting diagnostics
     * @return an object representing the compilation process
     * @throws IOException if temporary jar files cannot be created
     */
    public CompilationTask getTask(final Writer out,
            final DiagnosticCollector<JavaFileObject> digsCollector)
            throws IOException {
        if (m_compiler == null) {
            m_compileArgs = new ArrayList<>();
            File[] classpaths = m_snippet.getClassPath();
            if (classpaths != null && classpaths.length > 0) {
                m_compileArgs.add("-classpath");
                StringBuilder b = new StringBuilder();
                for (int i = 0; i < classpaths.length; i++) {
                    if (i > 0) {
                        b.append(File.pathSeparatorChar);
                    }
                    b.append(classpaths[i]);
                }
                m_compileArgs.add(b.toString());
            }
            m_compileArgs.add("-source");
            m_compileArgs.add("1.7");
            m_compileArgs.add("-target");
            m_compileArgs.add("1.7");

            m_compiler  = new EclipseCompiler();
        }
        try (StandardJavaFileManager stdFileMgr = m_compiler.getStandardFileManager(
                digsCollector, null, null);) {
            CompilationTask compileTask = m_compiler.getTask(out, stdFileMgr,
                digsCollector, m_compileArgs, null,
                    m_snippet.getCompilationUnits());
            return compileTask;
        }
    }

    /**
     * A class loader that can be used to load the compiled classes.
     *
     * @param parent the parent class loader
     * @return the class loader
     * @throws IOException if compiled classes cannot be accessed.
     */
    public ClassLoader createClassLoader(final ClassLoader parent)
        throws IOException {
        File[] classpaths = m_snippet.getClassPath();
        URL[] urls = new URL[classpaths.length + 1];
        if (classpaths.length > 0) {
            for (int i = 0; i < classpaths.length; i++) {
                try {
                    urls[i] = classpaths[i].toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalStateException("Unable to retrieve "
                            + "URL from jar file \""
                            + classpaths[i].getAbsolutePath() + "\"", e);
                }
            }
        }
        urls[urls.length - 1] = m_snippet.getTempClassPath().toURI().toURL();
        return URLClassLoader.newInstance(urls, parent);
    }

}
