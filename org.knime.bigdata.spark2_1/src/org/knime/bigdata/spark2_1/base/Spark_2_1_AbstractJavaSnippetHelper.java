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
 */
package org.knime.bigdata.spark2_1.base;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.text.BadLocationException;

import org.eclipse.osgi.internal.loader.EquinoxClassLoader;
import org.eclipse.osgi.internal.loader.classpath.ClasspathEntry;
import org.eclipse.osgi.internal.loader.classpath.ClasspathManager;
import org.eclipse.osgi.internal.loader.sources.PackageSource;
import org.eclipse.osgi.internal.loader.sources.SingleSourcePackage;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
import org.knime.base.node.jsnippet.util.JavaSnippetFields;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.base.node.jsnippet.util.field.InCol;
import org.knime.base.node.jsnippet.util.field.InVar;
import org.knime.base.node.jsnippet.util.field.JavaField;
import org.knime.base.node.jsnippet.util.field.OutCol;
import org.knime.base.node.jsnippet.util.field.OutVar;
import org.knime.bigdata.spark.node.scripting.java.util.helper.DefaultJavaSnippetHelper;
import org.knime.bigdata.spark2_1.api.Spark_2_1_CompatibilityChecker;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedDocument;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedSection;

/**
 * @author Bjoern Lohrmann, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings("restriction")
public abstract class Spark_2_1_AbstractJavaSnippetHelper extends DefaultJavaSnippetHelper {

    /** internal logger */
    protected static final NodeLogger LOGGER = NodeLogger.getLogger(Spark_2_1_AbstractJavaSnippetHelper.class);

    private final static List<File> classpathSingleton = new LinkedList<File>();

    /** Default constructor */
    public Spark_2_1_AbstractJavaSnippetHelper() {
        super(Spark_2_1_CompatibilityChecker.INSTANCE);
    }

    @Override
    public GuardedDocument createGuardedSnippetDocument(final SnippetType type, final JavaSnippetSettings settings) {
        final String methodSignature = getMethodSignature(type);
        GuardedDocument doc = new JavaSnippetDocument(methodSignature);
        updateAllSections(doc, getSnippetClassName(type), getSnippetSuperClass(type), methodSignature, settings);
        return doc;
    }

    @Override
    public void updateGuardedSections(final SnippetType type, final GuardedDocument doc,
        final JavaSnippetFields fields) {

        updateGuardedSections(doc, fields, getSnippetClassName(type), getSnippetSuperClass(type), getMethodSignature(type));
    }

    @Override
    public void updateAllSections(final SnippetType type, final GuardedDocument doc,
        final JavaSnippetSettings settings) {

        updateAllSections(doc, getSnippetClassName(type), getSnippetSuperClass(type), getMethodSignature(type), settings);
    }

    private void updateAllSections(final GuardedDocument doc, final String snippetClassName,
        final Class<?> snippetSuperClass, final String methodSignature, final JavaSnippetSettings settings) {

        try {
            updateGuardedSections(doc, settings.getJavaSnippetFields(), snippetClassName, snippetSuperClass, methodSignature);
            updateFreetextSections(doc, settings);
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void updateFreetextSections(final GuardedDocument doc, final JavaSnippetSettings settings)
        throws BadLocationException {
        doc.replaceBetween(JavaSnippetDocument.GUARDED_IMPORTS, JavaSnippetDocument.GUARDED_FIELDS,
            settings.getScriptImports());
        doc.replaceBetween(JavaSnippetDocument.GUARDED_FIELDS, JavaSnippetDocument.GUARDED_BODY_START,
            settings.getScriptFields());
        doc.replaceBetween(JavaSnippetDocument.GUARDED_BODY_START, JavaSnippetDocument.GUARDED_BODY_END,
            settings.getScriptBody());
    }


    /**
     * Updates the guarded sections of the given {@link GuardedDocument}.
     */
    private void updateGuardedSections(final GuardedDocument doc, final JavaSnippetFields fields,
        final String snippetClassName, final Class<?> snippetSuperClass, final String methodSignature) {

        try {
            GuardedSection guardedImports = doc.getGuardedSection(JavaSnippetDocument.GUARDED_IMPORTS);
            guardedImports.setText(createImportsSection());

            GuardedSection guardedBodyStart = doc.getGuardedSection(JavaSnippetDocument.GUARDED_BODY_START);
            guardedBodyStart.setText(createBodyStartSection(methodSignature));

            GuardedSection guardedFields = doc.getGuardedSection(JavaSnippetDocument.GUARDED_FIELDS);
            guardedFields.setText(createFieldsSection(fields, snippetClassName, snippetSuperClass));
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Create body start section (containing the method signature) of the snippet.
     */
    private String createBodyStartSection(final String methodSignature) {
        return "// expression start\n    " + methodSignature + " {\n";
    }

    /**
     * Create the system variable (input and output) section of the snippet.
     */
    private String createFieldsSection(final JavaSnippetFields fields, final String snippetClassName,
        final Class<?> snippetSuperClass) {
        StringBuilder out = new StringBuilder();
        out.append("// system variables\n");
        out.append(String.format("public class %s extends %s {\n\tprivate static final long serialVersionUID = 1L;\n",
            snippetClassName, snippetSuperClass.getSimpleName()));

        if (fields.getInColFields().size() > 0) {
            out.append("  // Fields for input columns\n");
            for (InCol field : fields.getInColFields()) {
                out.append(String.format("/** Input column: \"%s\" */\n", field.getKnimeName()));
                appendFields(out, field);
            }
        }
        if (fields.getInVarFields().size() > 0) {
            out.append("  // Fields for input flow variables\n");
            for (InVar field : fields.getInVarFields()) {
                out.append(String.format("/** Input flow variable: \"%s\" */\n", field.getKnimeName()));
                appendFields(out, field);
            }
        }
        out.append("\n");
        if (fields.getOutColFields().size() > 0) {
            out.append("  // Fields for output columns\n");
            for (OutCol field : fields.getOutColFields()) {
                out.append(String.format("/** Output column: \"%s\" */\n", field.getKnimeName()));
                appendFields(out, field);
            }
        }
        if (fields.getOutVarFields().size() > 0) {
            out.append("  // Fields for output flow variables\n");
            for (OutVar field : fields.getOutVarFields()) {
                out.append("/** Output flow variable: \"");
                out.append(field.getKnimeName());
                out.append("\" */\n");
                appendFields(out, field);
            }
        }

        out.append("\n");
        return out.toString();
    }

    /** Append field declaration to the string builder. */
    private void appendFields(final StringBuilder out, final JavaField f) {
        out.append("  public ");
        if (null != f.getJavaType()) {
            out.append(f.getJavaType().getSimpleName());
        } else {
            out.append("<invalid>");
        }

        out.append(" ");
        out.append(f.getJavaName());
        out.append(";\n");
    }

    /**
     * Create the imports section for the snippet's document.
     */
    private String createImportsSection() {
        StringBuilder imports = new StringBuilder();
        imports.append("// system imports\n");
        for (String s : getSystemImports()) {
            imports.append("import ");
            imports.append(s);
            imports.append(";\n");
        }
        imports.append("\n");
        return imports.toString();
    }

    /**
     * Get the list of default imports. Override this method to append or modify this list.
     *
     * @return the list of default imports
     */
    protected abstract String[] getSystemImports();

    @Override
    public String getSnippetClassName(final SnippetType type) {
        return getSnippetClassName(type, "");
    }

    @Override
    public List<File> getSnippetClasspath() {

        // use cached list if present
        synchronized (classpathSingleton) {
            if (classpathSingleton.isEmpty()) {
                initSnippetClasspath();
            }
        }

        return classpathSingleton;
    }

    private void initSnippetClasspath() {

        // Each of these packages is accessible from spark2_1 plugin
        // but comes from a different plugin dependency.
        // Below we use these packages to locate their physical
        // location, i.e. their jar files and class folders to build the classpath
        // for java snippet compilation.
        final String[] packages = {
            "scala",
            "scala.reflect.api",
            "org.apache.spark",
            "com.google.common.base",
            "org.knime.bigdata.spark.core.exception",
            "org.apache.hadoop.conf",
            "org.apache.hadoop.mapred"
        };

        final EquinoxClassLoader cl = (EquinoxClassLoader)getClass().getClassLoader();

        final Set<String> classpathEntries = new HashSet<>();

        // scan over the imports and add all classpath entries that
        // provide the classes from the imports (this adds org.knime.bigdata.spark.core and the spark libs)
        for (String pkg : packages) {
            final PackageSource packageSource = cl.getBundleLoader().getPackageSource(pkg);

            if (packageSource == null) {
                LOGGER.debug(String.format("%s: Could not find source for package %s", cl.getBundle().getSymbolicName(), pkg));
                continue;
            }

            for (SingleSourcePackage supplier : packageSource.getSuppliers()) {
                for (ClasspathEntry cpEntry : supplier.getLoader().getModuleClassLoader().getClasspathManager()
                    .getHostClasspathEntries()) {
                    classpathEntries.add(cpEntry.getBundleFile().getBaseFile().getAbsolutePath());
                }
            }
        }

        // also, we need to add the classpath entries of this Spark versio-specific plugin
        final ClasspathManager cpm = cl.getClasspathManager();
        final ClasspathEntry[] ce = cpm.getHostClasspathEntries();
        for (final ClasspathEntry classpathEntry : ce) {
            final File classpathEntryFile = classpathEntry.getBundleFile().getBaseFile();
            classpathEntries.add(classpathEntryFile.getAbsolutePath());
        }

        // now turn the set of paths into a list of, adjusting for bin/ directories
        for (String classpathEntry : classpathEntries) {
            final File entryFile = new File(classpathEntry);

            if (entryFile.isDirectory() && new File(entryFile, "bin").isDirectory()) {
                LOGGER.debug(String.format("%s: Adding %s/bin to snippet classpath", cl.getBundle().getSymbolicName(),
                    classpathEntry));
                classpathSingleton.add(new File(entryFile, "bin"));
            } else {
                LOGGER.debug(String.format("%s: Adding %s to snippet classpath", cl.getBundle().getSymbolicName(),
                    classpathEntry));
                classpathSingleton.add(entryFile);
            }
        }
    }

    /**
     * Get snippet super class.
     *
     * @param type source/snippet/sink
     * @return snippet super class
     */
    protected abstract Class<?> getSnippetSuperClass(final SnippetType type);

    @Override
    public void updateGuardedClassnameSuffix(final SnippetType type, final GuardedDocument doc,
        final JavaSnippetFields fields, final String classnameSuffix) {

        updateGuardedSections(doc, fields, getSnippetClassName(type, classnameSuffix), getSnippetSuperClass(type), getMethodSignature(type));
    }
}
