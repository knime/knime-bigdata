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
 *   Created on May 6, 2016 by bjoern
 */
package org.knime.bigdata.spark1_5.base;

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
import org.knime.bigdata.spark1_5.api.Spark_1_5_CompatibilityChecker;
import org.knime.bigdata.spark1_5.jobs.scripting.java.AbstractSparkJavaSnippet;
import org.knime.bigdata.spark1_5.jobs.scripting.java.AbstractSparkJavaSnippetSink;
import org.knime.bigdata.spark1_5.jobs.scripting.java.AbstractSparkJavaSnippetSource;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedDocument;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedSection;

/**
 * @author Bjoern Lohrmann, KNIME.com
 */
@SuppressWarnings("restriction")
public class Spark_1_5_JavaSnippetHelper extends DefaultJavaSnippetHelper {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark_1_5_JavaSnippetHelper.class);

    private static final Class<?> INNER_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippet.class;

    private static final Class<?> SOURCE_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippetSource.class;

    private static final Class<?> SINK_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippetSink.class;

    private final static String INNER_SNIPPET_METHOD_SIG =
        "public JavaRDD<Row> apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD1, final JavaRDD<Row> rowRDD2)"
            + " throws Exception";

    private final static String SOURCE_SNIPPET_METHOD_SIG =
        "public JavaRDD<Row> apply(final JavaSparkContext sc) " + "throws Exception";

    private final static String SINK_SNIPPET_METHOD_SIG =
        "public void apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD)" + " throws Exception";

    private final static String INNER_SNIPPET_DEFAULT_CONTENT = "return rowRDD1;";

    private final static String SINK_SNIPPET_DEFAULT_CONTENT = "//sink";

    private final static String SOURCE_SNIPPET_DEFAULT_CONTENT = "return sc.<Row>emptyRDD();";

    private final static String INNER_SNIPPET_CLASSNAME = "SparkJavaSnippet";

    private final static String SOURCE_SNIPPET_CLASSNAME = "SparkJavaSnippetSource";

    private final static String SINK_SNIPPET_CLASSNAME = "SparkJavaSnippetSink";

    private final static List<File> classpathSingleton = new LinkedList<File>();

    /**
     * Constructor.
     */
    public Spark_1_5_JavaSnippetHelper() {
        super(Spark_1_5_CompatibilityChecker.INSTANCE);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAllSections(final SnippetType type, final GuardedDocument doc,
        final JavaSnippetSettings settings) {

        updateAllSections(doc, getSnippetClassName(type), getSnippetSuperClass(type), getMethodSignature(type), settings);
    }


    private static void updateAllSections(final GuardedDocument doc, final String snippetClassName,
        final Class<?> snippetSuperClass, final String methodSignature, final JavaSnippetSettings settings) {

        try {
            updateGuardedSections(doc, settings.getJavaSnippetFields(), snippetClassName, snippetSuperClass, methodSignature);
            updateFreetextSections(doc, settings);
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }


    private static void updateFreetextSections(final GuardedDocument doc, final JavaSnippetSettings settings)
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
    private static void updateGuardedSections(final GuardedDocument doc, final JavaSnippetFields fields,
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
    private static String createBodyStartSection(final String methodSignature) {
        return "// expression start\n    " + methodSignature + " {\n";
    }

    /**
     * Create the system variable (input and output) section of the snippet.
     */
    private static String createFieldsSection(final JavaSnippetFields fields, final String snippetClassName,
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
    private static void appendFields(final StringBuilder out, final JavaField f) {
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
    private static String createImportsSection() {
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
    protected static String[] getSystemImports() {
        return new String[]{"org.apache.spark.SparkContext", "org.apache.spark.api.java.JavaSparkContext",
            "org.apache.spark.api.java.*", "org.apache.spark.api.java.function.*", "org.apache.spark.sql.types.*","org.apache.spark.sql.*",
            "org.knime.bigdata.spark.core.exception.*",
            "org.knime.bigdata.spark1_5.api.RowBuilder",
            INNER_SNIPPET_SUPERCLASS.getName(),
            SOURCE_SNIPPET_SUPERCLASS.getName(), SINK_SNIPPET_SUPERCLASS.getName()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSnippetClassName(final SnippetType type) {
        return getSnippetClassName(type, "");
    }

    /**
     * {@inheritDoc}
     */
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

        // Each of these packages is accessible from spark1_5 plugin
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

    private static Class<?> getSnippetSuperClass(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_SUPERCLASS;
            case SOURCE:
                return SOURCE_SNIPPET_SUPERCLASS;
            case SINK:
                return SINK_SNIPPET_SUPERCLASS;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSnippetClassName(final SnippetType type, final String suffix) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_CLASSNAME + suffix;
            case SOURCE:
                return SOURCE_SNIPPET_CLASSNAME + suffix;
            case SINK:
                return SINK_SNIPPET_CLASSNAME + suffix;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateGuardedClassnameSuffix(final SnippetType type, final GuardedDocument doc,
        final JavaSnippetFields fields, final String classnameSuffix) {

        updateGuardedSections(doc, fields, getSnippetClassName(type, classnameSuffix), getSnippetSuperClass(type), getMethodSignature(type));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDefaultContent(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_DEFAULT_CONTENT;
            case SOURCE:
                return SOURCE_SNIPPET_DEFAULT_CONTENT;
            case SINK:
                return SINK_SNIPPET_DEFAULT_CONTENT;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    @Override
    public String getMethodSignature(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_METHOD_SIG;
            case SOURCE:
                return SOURCE_SNIPPET_METHOD_SIG;
            case SINK:
                return SINK_SNIPPET_METHOD_SIG;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }
}
