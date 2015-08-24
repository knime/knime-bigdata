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
 *   Created on 19.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.java.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

import org.eclipse.jdt.internal.compiler.tool.EclipseFileObject;
import org.fife.ui.rsyntaxtextarea.parser.Parser;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
import org.knime.base.node.jsnippet.ui.JSnippetParser;
import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.JSnippet;
import org.knime.base.node.jsnippet.util.JavaField;
import org.knime.base.node.jsnippet.util.JavaField.InCol;
import org.knime.base.node.jsnippet.util.JavaField.InVar;
import org.knime.base.node.jsnippet.util.JavaField.OutCol;
import org.knime.base.node.jsnippet.util.JavaField.OutVar;
import org.knime.base.node.jsnippet.util.JavaSnippetCompiler;
import org.knime.base.node.jsnippet.util.JavaSnippetFields;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.base.node.jsnippet.util.JavaSnippetUtil;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.FileUtil;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.jobs.AbstractSparkJavaSnippet;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;

/**
 * {@link JSnippet} implementation for the SPark Java Snippet nodes.
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippet implements JSnippet<SparkJavaSnippetTemplate> {
    private static File jSnippetJar;
    private String[] m_jarFiles;
    // caches the jSnippetJar and the jarFiles.
    private File[] m_jarFileCache;

    private JavaFileObject m_snippet;
    private File m_snippetFile;

    private GuardedDocument m_document;
    // true when the document has changed and the m_snippet is not up to date.
    private boolean m_dirty;

    private JSnippetParser m_parser;

    private JavaSnippetSettings m_settings;
    private JavaSnippetFields m_fields;


    private final File m_tempClassPathDir;

//    private NodeLogger m_logger;
    private String m_className;
    private Class<? extends KnimeSparkJob> m_abstractClass;
    private String m_methodSignature;

    /**
     * Create a new snippet.
     * @param className the name of the class
     * @param abstractClass the KNIMESparkJob this class should extend
     * @param methodSignature
     */
    public SparkJavaSnippet(final String className, final Class<? extends KnimeSparkJob> abstractClass,
        final String methodSignature) {
        m_className = className;
        m_abstractClass = abstractClass;
        m_methodSignature = methodSignature;
        m_fields = new JavaSnippetFields();
        File tempDir;
        try {
            tempDir = FileUtil.createTempDir("knime_sparkjavasnippet");
        } catch (IOException ex) {
            NodeLogger.getLogger(getClass()).error(
                "Could not create temporary directory for Java Snippet: " + ex.getMessage(), ex);
            // use the standard temp directory instead
            tempDir = new File(KNIMEConstants.getKNIMETempDir());
        }
        m_tempClassPathDir = tempDir;
    }

    /**
     * Create a new snippet with the given settings.
     * @param settings the settings
     */
    public void setSettings(final JavaSnippetSettings settings) {
        m_settings = settings;
        setJavaSnippetFields(settings.getJavaSnippetFields());
        setJarFiles(settings.getJarFiles());
        init();
    }

    private void init() {
        if (null != m_document) {
            initDocument(m_document);
        }
    }


    /**
     * Get the updated settings java snippet.
     * @return the settings
     */
    public JavaSnippetSettings getSettings() {
        updateSettings();
        return m_settings;
    }

    private void updateSettings() {
        try {
            GuardedDocument doc = getDocument();
            m_settings.setScriptImports(doc.getTextBetween(JavaSnippetDocument.GUARDED_IMPORTS,
                JavaSnippetDocument.GUARDED_FIELDS));
            m_settings.setScriptFields(doc.getTextBetween(JavaSnippetDocument.GUARDED_FIELDS,
                JavaSnippetDocument.GUARDED_BODY_START));
            m_settings.setScriptBody(doc.getTextBetween(JavaSnippetDocument.GUARDED_BODY_START,
                JavaSnippetDocument.GUARDED_BODY_END));
        } catch (BadLocationException e) {
            // this should never happen
            throw new IllegalStateException(e);
        }
        // update input fields
        m_settings.setJavaSnippetFields(m_fields);
        // update jar files
        m_settings.setJarFiles(m_jarFiles);
    }


    /**
     * Get the jar files to be added to the class path.
     * @return the jar files for the class path
     * @throws IOException when a file could not be loaded
     */
    @Override
    public File[] getClassPath() throws IOException {
        // use cached list if present
        if (filesExist(m_jarFileCache)) {
            return m_jarFileCache;
        }
        // Add lock since jSnippetJar is used across all JavaSnippets
        synchronized (SparkJavaSnippet.class) {
            if (jSnippetJar == null || !jSnippetJar.exists()) {
                jSnippetJar = createJSnippetJarFile();
            }
        }
        //TK_TODO: Get the jar files dynamically
            final List<File> jarFiles = new ArrayList<>();
            //add the default jar files to the class path which are also available on the Spark cluster
            final String root = SparkPlugin.getDefault().getPluginRootPath();
//            jarFiles.add(new File(root+"/bin/"));
            jarFiles.add(new File(root+"/lib/jobServerUtilsApi.jar"));
//            jarFiles.add(new File(root+"/lib/scala-library.jar"));
//            jarFiles.add(new File(root+"/lib/scala-reflect.jar"));
//            jarFiles.add(new File(root+"/lib/spark-core_2.10-1.2.2.jar"));
//            jarFiles.add(new File(root+"/lib/spark-mllib_2.10-1.2.2.jar"));
//            jarFiles.add(new File(root+"/lib/spark-sql_2.10-1.2.2.jar"));
//            jarFiles.add(new File(root+"/lib/spark-sql-api.jar"));
//            jarFiles.add(new File(root+"/lib/typesafe-config.jar"));
            final File libDir = new File(root+"/lib");
            final String[] libJarNames = libDir.list(new FilenameFilter() {
                @Override
                public boolean accept(final File dir, final String name) {
                    return name.endsWith(".jar");
                }
            });
            for (String jarName : libJarNames) {
                jarFiles.add(new File(root+"/lib/"+jarName));
            }
            jarFiles.add(new File(root+"/resources/knimeJobs.jar"));
            jarFiles.add(jSnippetJar);
            if (null != m_jarFiles && m_jarFiles.length > 0) {
                for (int i = 0; i < m_jarFiles.length; i++) {
                    try {
                        jarFiles.add(JavaSnippetUtil.toFile(m_jarFiles[i]));
                    } catch (InvalidSettingsException e) {
                        // jar file does not exist
                        // TODO how to react?
                    }
                }
            }
            m_jarFileCache = jarFiles.toArray(new File[jarFiles.size()]);
//        } else {
//            m_jarFileCache = new File[] {jSnippetJar};
//        }
        return m_jarFileCache;
    }

    /**
     * Tests if files in the given array exist.
     * @param files the files to test
     * @return true if array is not null and all files exist.
     */
    private boolean filesExist(final File[] files) {
        if (null == files) {
            return false;
        }
        boolean exists = true;
        for (File file : files) {
            exists = exists && file.exists();
        }
        return exists;
    }

    /**
     * Get compilation units used by the JavaSnippetCompiler.
     * @return the files to compile
     * @throws IOException When files cannot be created.
     */
    @Override
    public Iterable<? extends JavaFileObject> getCompilationUnits()
        throws IOException {

        if (m_snippet == null || m_snippetFile == null
                || !m_snippetFile.exists()) {
            m_snippet = createJSnippetFile();
        } else {
            if (m_dirty) {
                try(final Writer out = m_snippet.openWriter();) {
                    try {
                        Document doc = getDocument();
                        out.write(doc.getText(0, doc.getLength()));
                        m_dirty = false;
                    } catch (BadLocationException e) {
                        // this should never happen.
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
        return Collections.singletonList(m_snippet);
    }

    /**
     * Create the java-file of the snippet.
     */
    @SuppressWarnings("restriction")
    private JavaFileObject createJSnippetFile() throws IOException {
        m_snippetFile = new File(m_tempClassPathDir, m_className + ".java");
        try (FileOutputStream fos = new FileOutputStream(m_snippetFile);
            OutputStreamWriter out = new OutputStreamWriter(fos, Charset.forName("UTF-8"));){
            Document doc = getDocument();
            out.write(doc.getText(0, doc.getLength()));
        } catch (BadLocationException e) {
            // this should never happen.
            throw new IllegalStateException(e);
        }

        return new EclipseFileObject("JSnippet", m_snippetFile.toURI(),
                Kind.SOURCE, Charset.defaultCharset());
    }


    /**
     * Return true when this snippet is the creator and maintainer of the
     * given source.
     * @param source the source
     * @return if this snippet is the given source
     */
    @Override
    public boolean isSnippetSource(final JavaFileObject source) {
        return null != m_snippet ? source.equals(m_snippet) : false;
    }


    /**
     * Give jar file with all *.class files returned by
     * getManipulators(ALL_CATEGORY).
     *
     * @return file object of a jar file with all compiled manipulators
     * @throws IOException if jar file cannot be created
     */
    private File createJSnippetJarFile() throws IOException {
        File jarFile = FileUtil.createTempFile("sparkjavasnippet", ".jar",
            new File(KNIMEConstants.getKNIMETempDir()), true);
        try (JarOutputStream jar = new JarOutputStream(new FileOutputStream(jarFile));) {
//TK_TODO: add the required class files to the jar
            Collection<Object> classes = new ArrayList<>();
            classes.add(m_abstractClass);

//            classes.add(Abort.class);
//            classes.add(Cell.class);
//            classes.add(ColumnException.class);
//            classes.add(FlowVariableException.class);
//            classes.add(Type.class);
//            classes.add(TypeException.class);
//            classes.add(NodeLogger.class);
//            classes.add(KNIMEConstants.class);
            // create tree structure for classes
            DefaultMutableTreeNode root = createTree(classes);
            try {
                createJar(root, jar, null);
            } finally {
                jar.close();
            }
            return jarFile;
        }
    }


    private static DefaultMutableTreeNode createTree(
            final Collection<? extends Object> classes) {
        DefaultMutableTreeNode root = new DefaultMutableTreeNode("build");
        for (Object o : classes) {
            Class<?> cl = o instanceof Class ? (Class<?>)o : o.getClass();
            Package pack = cl.getPackage();
            DefaultMutableTreeNode curr = root;
            for (String p : pack.getName().split("\\.")) {
                DefaultMutableTreeNode child = getChild(curr, p);
                if (null == child) {
                    DefaultMutableTreeNode h = new DefaultMutableTreeNode(p);
                    curr.add(h);
                    curr = h;
                } else {
                    curr = child;
                }
            }
            curr.add(new DefaultMutableTreeNode(cl));
        }

        return root;
    }


    private static DefaultMutableTreeNode getChild(final DefaultMutableTreeNode curr,
            final String p) {
        for (int i = 0; i < curr.getChildCount(); i++) {
            DefaultMutableTreeNode child =
                (DefaultMutableTreeNode)curr.getChildAt(i);
            if (child.getUserObject().toString().equals(p)) {
                return child;
            }
        }
        return null;
    }


    private static void createJar(final DefaultMutableTreeNode node,
            final JarOutputStream jar,
            final String path) throws IOException {
        Object o = node.getUserObject();
        if (o instanceof String) {
            // folders must end with a "/"
            String subPath = null == path ? "" : (path + (String)o + "/");
            if (path != null) {
                JarEntry je = new JarEntry(subPath);
                jar.putNextEntry(je);
                jar.flush();
                jar.closeEntry();
            }
            for (int i = 0; i < node.getChildCount(); i++) {
                DefaultMutableTreeNode child =
                    (DefaultMutableTreeNode) node.getChildAt(i);
                createJar(child, jar, subPath);
            }
        } else {
            Class<?> cl = (Class<?>)o;
            String className = cl.getSimpleName();
            className = className.concat(".class");
            JarEntry entry = new JarEntry(path + className);
            jar.putNextEntry(entry);

            ClassLoader loader = cl.getClassLoader();
            try (InputStream inStream = loader.getResourceAsStream(
                    cl.getName().replace('.', '/') + ".class");) {
                FileUtil.copy(inStream, jar);
                inStream.close();
                jar.flush();
                jar.closeEntry();
            }
        }
    }


    /**
     * Get the list of default imports. Override this method to append or
     * modify this list.
     * @return the list of default imports
     */
    protected String[] getSystemImports() {
        //TK_TODO: Check the inports
//        String pkg = "org.knime.base.node.jsnippet.expression";
        return new String[] {//AbstractSparkJavaSnippet.class.getName(),
//                Abort.class.getName()
//                , Cell.class.getName()
//                , ColumnException.class.getName()
//                , TypeException.class.getName()
//                , "static " + pkg + ".Type.*"
//                , "java.util.Calendar"
                 "org.apache.spark.SparkContext"
                , "org.apache.spark.api.java.JavaSparkContext"
//                , "spark.jobserver.SparkJobValidation"
//                , "com.typesafe.config.Config"
//                , "com.knime.bigdata.spark.jobserver.server.JobResult"
//                , "com.knime.bigdata.spark.jobserver.server.ParameterConstants"
//                , "com.knime.bigdata.spark.jobserver.server.RDDUtils"
//                , "com.knime.bigdata.spark.jobserver.server.ValidationResultConverter"
                , "org.apache.spark.api.java.*"
                , "org.apache.spark.api.java.function.*"
                , "org.apache.spark.sql.api.java.*"
                , "com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException"
                , "com.knime.bigdata.spark.jobserver.server.transformation.*"
//                , "com.knime.bigdata.spark.node.scripting.util.*"
                , "java.io.Serializable"
                , m_abstractClass.getName()};
    }


    /**
     * Get the document with the code of the snippet.
     * @return the document
     */
    @Override
    public GuardedDocument getDocument() {
        // Lazy initialization of the document
        if (m_document == null) {
            m_document = createDocument();
            m_document.addDocumentListener(new DocumentListener() {

                @Override
                public void removeUpdate(final DocumentEvent e) {
                    m_dirty = true;
                }

                @Override
                public void insertUpdate(final DocumentEvent e) {
                    m_dirty = true;
                }

                @Override
                public void changedUpdate(final DocumentEvent e) {
                    m_dirty = true;
                }
            });
            initDocument(m_document);
        }
        return m_document;
    }


    /** Create the document with the default skeleton. */
    private GuardedDocument createDocument() {
        return new JavaSnippetDocument(m_methodSignature);
    }

    /** Initialize document with information from the settings. */
    private void initDocument(final GuardedDocument doc) {
        try {
            initGuardedSections(doc);
            if (null != m_settings) {
                doc.replaceBetween(JavaSnippetDocument.GUARDED_IMPORTS, JavaSnippetDocument.GUARDED_FIELDS,
                        m_settings.getScriptImports());
                doc.replaceBetween(JavaSnippetDocument.GUARDED_FIELDS, JavaSnippetDocument.GUARDED_BODY_START,
                        m_settings.getScriptFields());
                doc.replaceBetween(JavaSnippetDocument.GUARDED_BODY_START, JavaSnippetDocument.GUARDED_BODY_END,
                        m_settings.getScriptBody());
            }
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /** Initialize GUARDED_IMPORTS and GUARDED_FIELDS with information from
     * the settings.
     */
    private void initGuardedSections(final GuardedDocument doc) {
        try {
            GuardedSection imports = doc.getGuardedSection(JavaSnippetDocument.GUARDED_IMPORTS);
            imports.setText(createImportsSection());
            GuardedSection fields = doc.getGuardedSection(JavaSnippetDocument.GUARDED_FIELDS);
            fields.setText(createFieldsSection());
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }


    /**
     * Create the system variable (input and output) section of the snippet.
     */
    private String createFieldsSection() {
        StringBuilder out = new StringBuilder();
        out.append("// system variables\n");
        out.append("public class " + m_className + " extends " + m_abstractClass.getSimpleName()
            + " implements Serializable {\n\tprivate static final long serialVersionUID = 1L;\n");
        if (m_fields.getInColFields().size() > 0) {
            out.append("  // Fields for input columns\n");
            for (InCol field : m_fields.getInColFields()) {
                out.append("/** Input column: \"");
                out.append(field.getKnimeName());
                out.append("\" */\n");
                appendFields(out, field);
            }
        }
        if (m_fields.getInVarFields().size() > 0) {
            out.append("  // Fields for input flow variables\n");
            for (InVar field : m_fields.getInVarFields()) {
                out.append("/** Input flow variable: \"");
                out.append(field.getKnimeName());
                out.append("\" */\n");
                appendFields(out, field);
            }
        }
        out.append("\n");
        if (m_fields.getOutColFields().size() > 0) {
            out.append("  // Fields for output columns\n");
            for (OutCol field : m_fields.getOutColFields()) {
                out.append("/** Output column: \"");
                out.append(field.getKnimeName());
                out.append("\" */\n");
                appendFields(out, field);
            }
        }
        if (m_fields.getOutVarFields().size() > 0) {
            out.append("  // Fields for output flow variables\n");
            for (OutVar field : m_fields.getOutVarFields()) {
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
    private void appendFields(final StringBuilder out,
            final JavaField f) {
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
     * Get the parser for the snippet's document.
     * @return the parser
     */
    @Override
    public Parser getParser() {
        // lazy initialisation of the parser
        if (m_parser == null) {
            m_parser = new JSnippetParser(this);
        }
        return m_parser;
    }

    /**
     * Validate settings which is typically called in the configure method
     * of a node.
     * @param inSpecs the spec of the data table at the inport
     * @param flowVariableRepository the flow variables at the inport
     * @return the validation results
     */
    public ValidationReport validateSettings(final PortObjectSpec[] inSpecs,
        final FlowVariableRepository flowVariableRepository) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // check input fields
//        for (InCol field : m_fields.getInColFields()) {
//            int index = spec.findColumnIndex(field.getKnimeName());
//            if (index >= 0) {
//                DataColumnSpec colSpec = spec.getColumnSpec(index);
//                if (!colSpec.getType().equals(field.getKnimeType())) {
//                    TypeProvider provider = TypeProvider.getDefault();
//                    DataValueToJava converter = provider.getDataValueToJava(
//                            colSpec.getType(),
//                            field.getJavaType().isArray());
//                    if (converter.canProvideJavaType(field.getJavaType())) {
//                        warnings.add("The type of the column \""
//                                + field.getKnimeName()
//                                + "\" has changed but is compatible.");
//                    } else {
//                        errors.add("The type of the column \""
//                                + field.getKnimeName()
//                                + "\" has changed.");
//                    }
//                }
//            } else {
//                errors.add("The column \"" + field.getKnimeName()
//                        + "\" is not found in the input table.");
//            }
//        }
        // check input variables
        for (InVar field : m_fields.getInVarFields()) {
            FlowVariable var = flowVariableRepository.getFlowVariable(field.getKnimeName());
            if (var != null) {
                if (!var.getType().equals(field.getKnimeType())) {
                    errors.add("The type of the flow variable \""
                                + field.getKnimeName()
                                + "\" has changed.");
                }
            } else {
                errors.add("The flow variable \"" + field.getKnimeName()
                        + "\" is not found in the input.");
            }
        }

        // check output fields
//        for (OutCol field : m_fields.getOutColFields()) {
//            int index = spec.findColumnIndex(field.getKnimeName());
//            if (field.getReplaceExisting() && index < 0) {
//                errors.add("The output column \""
//                        + field.getKnimeName()
//                        + "\" is marked to be a replacement, "
//                        + "but an input with this name does not exist.");
//            }
//            if (!field.getReplaceExisting() && index > 0) {
//                errors.add("The output column \""
//                        + field.getKnimeName()
//                        + "\" is marked to be new, "
//                        + "but an input with this name does exist.");
//            }
//        }

        // check output variables
//        for (OutVar field : m_fields.getOutVarFields()) {
//            FlowVariable var = flowVariableRepository.getFlowVariable(field.getKnimeName());
//            if (field.getReplaceExisting() && var == null) {
//                errors.add("The output flow variable \""
//                        + field.getKnimeName()
//                        + "\" is marked to be a replacement, "
//                        + "but an input with this name does not exist.");
//            }
//            if (!field.getReplaceExisting() && var != null) {
//                errors.add("The output flow variable \""
//                        + field.getKnimeName()
//                        + "\" is marked to be new, "
//                        + "but an input with this name does exist.");
//            }
//        }

        try {
            // test if snippet compiles and if the file can be created
            createSnippetClass();
        } catch (Exception e) {
            errors.add(e.getMessage());
        }
        return new ValidationReport(errors.toArray(new String[errors.size()]),
                warnings.toArray(new String[warnings.size()]));
    }

    /**
     * Create the outspec of the java snippet node. This method is typically
     * used in the configure of a node.
     * @param inSpecs the spec of the data table at the inport
     * @param flowVariableRepository the flow variables at the inport
     * @return the spec at the output
     * @throws InvalidSettingsException when settings are inconsistent with
     *  the spec or the flow variables at the inport
     */
    public DataTableSpec configure(final PortObjectSpec[] inSpecs,
            final FlowVariableRepository flowVariableRepository)
            throws InvalidSettingsException {
//        DataTableSpec outSpec =
//            createRearranger(inSpecs, flowVariableRepository, -1).createSpec();
//        // populate flowVariableRepository with new flow variables having
//        // default values
//        for (OutVar outVar : m_fields.getOutVarFields()) {
//            FlowVariable flowVar = null;
//            if (outVar.getKnimeType().equals(
//                    org.knime.core.node.workflow.FlowVariable.Type.INTEGER)) {
//                flowVar = new FlowVariable(outVar.getKnimeName(), -1);
//            } else if (outVar.getKnimeType().equals(
//                    org.knime.core.node.workflow.FlowVariable.Type.DOUBLE)) {
//                flowVar = new FlowVariable(outVar.getKnimeName(), -1.0);
//            } else {
//                flowVar = new FlowVariable(outVar.getKnimeName(), "");
//            }
//            flowVariableRepository.put(flowVar);
//        }
//        return outSpec;
        return null;
    }

    /**
     * Create a template for this snippet.
     * @param metaCategory the meta category of the template
     * @return the template with a new uuid.
     */
    @Override
    @SuppressWarnings("rawtypes")
    public SparkJavaSnippetTemplate createTemplate(final Class metaCategory) {
        SparkJavaSnippetTemplate template = new SparkJavaSnippetTemplate(metaCategory, getSettings());
        return template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File getTempClassPath() {
        return m_tempClassPathDir;
    }

    /**
     * Get the system fields of the snippet.
     * @return the fields
     */
    public JavaSnippetFields getSystemFields() {
        return m_fields;
    }

    /**
     * Set the system fields in the java snippet.
     * @param fields the fields to set
     */
    @Override
    public void setJavaSnippetFields(final JavaSnippetFields fields) {
        m_fields = fields;
        if (null != m_document) {
            initGuardedSections(m_document);
        }
    }

    /**
     * Set the list of additional jar files to be added to the class path
     * of the snippet.
     * @param jarFiles the jar files
     */
    public void setJarFiles(final String[] jarFiles) {
        m_jarFiles = jarFiles.clone();
        // reset cache
        m_jarFileCache = null;
    }

    /**
     * Create the class file of the snippet.
     * @return the compiled snippet
     */
    @SuppressWarnings("unchecked")
    private Class<? extends AbstractSparkJavaSnippet> createSnippetClass() {
        JavaSnippetCompiler compiler = new JavaSnippetCompiler(this);
        StringWriter log = new StringWriter();
        DiagnosticCollector<JavaFileObject> digsCollector = new DiagnosticCollector<>();
        CompilationTask compileTask = null;
        try {
            compileTask = compiler.getTask(log, digsCollector);
        } catch (IOException e) {
            throw new IllegalStateException("Compile with errors", e);
        }
        boolean success = compileTask.call();
        if (success) {
            try {
                ClassLoader loader = compiler.createClassLoader(this.getClass().getClassLoader());
                return (Class<? extends AbstractSparkJavaSnippet>) loader.loadClass(m_className);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not load class file.", e);
            } catch (IOException e) {
                throw new IllegalStateException("Could not load jar files.", e);
            }
        } else {
            final StringBuilder msg = new StringBuilder();
            msg.append("Compile with errors:\n");
            for (Diagnostic<? extends JavaFileObject> d : digsCollector.getDiagnostics()) {
                boolean isSnippet = this.isSnippetSource(d.getSource());
                if (isSnippet && d.getKind().equals(javax.tools.Diagnostic.Kind.ERROR)) {
                    msg.append("Error: ");
                    msg.append(d.getMessage(Locale.US));
                    msg.append('\n');
                }
            }
            throw new IllegalStateException(msg.toString());
        }
    }

    /**
     * Create an instance of the snippet.
     * @return a snippet instance
     */
    AbstractSparkJavaSnippet createSnippetInstance() {
        Class<? extends AbstractSparkJavaSnippet> jsnippetClass = createSnippetClass();
        AbstractSparkJavaSnippet instance;
        try {
            instance = jsnippetClass.newInstance();
        } catch (InstantiationException e) {
            // cannot happen, but rethrow just in case
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            // cannot happen, but rethrow just in case
            throw new RuntimeException(e);
        }
//        if (m_logger != null) {
//            instance.attachLogger(m_logger);
//        }
        return instance;
    }


//    /**
//     * Attach logger to be used by this java snippet instance.
//     * @param logger the node logger
//     */
//    public void attachLogger(final NodeLogger logger) {
//        m_logger = logger;
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finalize() throws Throwable {
        FileUtil.deleteRecursively(m_tempClassPathDir);
        super.finalize();
    }


    /**
     * @return the name of the class
     */
    public String getClassName() {
        return m_className;
    }
}
