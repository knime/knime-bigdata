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
 *   Created on 19.06.2015 by koetter
 */
package org.knime.bigdata.spark.node.scripting.java.util;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

import org.eclipse.jdt.internal.compiler.tool.EclipseFileObject;
import org.fife.ui.rsyntaxtextarea.parser.Parser;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
import org.knime.base.node.jsnippet.ui.JSnippetParser;
import org.knime.base.node.jsnippet.util.JSnippet;
import org.knime.base.node.jsnippet.util.JavaSnippetCompiler;
import org.knime.base.node.jsnippet.util.JavaSnippetFields;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.scripting.java.util.helper.AbstractJavaSnippetHelperRegistry;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import org.knime.bigdata.spark.node.scripting.java.util.template.SparkJavaSnippetTemplate;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedDocument;
import org.knime.core.util.FileUtil;

/**
 * {@link JSnippet} implementation for the Spark Java Snippet nodes. This class links the GUI components to the node
 * settings.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SuppressWarnings("restriction")
public class SparkJSnippet implements JSnippet<SparkJavaSnippetTemplate> {

    private SnippetType m_snippetType;

    private final AbstractJavaSnippetHelperRegistry m_helperRegistry;

    private JavaSnippetHelper m_helper;

    private JavaSnippetSettings m_settings;

    private final GuardedDocument m_document;

    private final JSnippetParser m_parser;

    /** Lazily initialized */
    private File m_snippetDir;

    /** Lazily initialized */
    private JavaFileObject m_snippetFile;

    private boolean m_snippetFileDirty;

    // true when the document has changed and the m_settings is not up to date.
    private boolean m_settingsDirty;

    private File[] m_classpathCache;

    /**
     * Create a new snippet with the default content for the given spark version and snippet type.
     *
     * @param sparkVersion
     * @param snippetType
     * @param helperRegistry
     */
    public SparkJSnippet(final SparkVersion sparkVersion, final SnippetType snippetType, final AbstractJavaSnippetHelperRegistry helperRegistry) {
        this(sparkVersion, snippetType, new JavaSnippetSettings(
            helperRegistry.getHelper(sparkVersion).getDefaultContent(snippetType)), helperRegistry);
    }

    /**
     * Create a new snippet with all content taken from the given settings.
     *
     * @param sparkVersion the name of the class
     * @param snippetType
     * @param settings
     * @param helperRegistry
     */
    public SparkJSnippet(final SparkVersion sparkVersion, final SnippetType snippetType,
        final JavaSnippetSettings settings, final AbstractJavaSnippetHelperRegistry helperRegistry) {

        m_snippetType = snippetType;
        m_helperRegistry = helperRegistry;
        m_helper = helperRegistry.getHelper(sparkVersion);
        m_settings = settings;

        m_document = m_helper.createGuardedSnippetDocument(snippetType, settings);
        m_document.addDocumentListener(new DocumentListener() {

            @Override
            public void removeUpdate(final DocumentEvent e) {
                m_settingsDirty = true;
                m_snippetFileDirty = true;
            }

            @Override
            public void insertUpdate(final DocumentEvent e) {
                m_settingsDirty = true;
                m_snippetFileDirty = true;
            }

            @Override
            public void changedUpdate(final DocumentEvent e) {
                m_settingsDirty = true;
                m_snippetFileDirty = true;
            }
        });

        m_parser = new JSnippetParser(this);
        m_snippetFileDirty = true;
    }

    /**
     * Get the updated settings java snippet.
     *
     * @return the settings
     */
    public JavaSnippetSettings getSettings() {
        if (m_settingsDirty) {
            updateSettingsFromDocument();
            m_settingsDirty = false;
        }

        return m_settings;
    }

    private void updateSettingsFromDocument() {
        try {
            m_settings.setScriptImports(
                m_document.getTextBetween(JavaSnippetDocument.GUARDED_IMPORTS, JavaSnippetDocument.GUARDED_FIELDS));
            m_settings.setScriptFields(
                m_document.getTextBetween(JavaSnippetDocument.GUARDED_FIELDS, JavaSnippetDocument.GUARDED_BODY_START));
            m_settings.setScriptBody(m_document.getTextBetween(JavaSnippetDocument.GUARDED_BODY_START,
                JavaSnippetDocument.GUARDED_BODY_END));
        } catch (BadLocationException e) {
            // this should never happen
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param sparkVersion the possibly new Spark version
     */
    public void setSparkVersion(final SparkVersion sparkVersion) {
        if (!m_helper.supportSpark(sparkVersion)) {
            updateDocumentFromSettings(sparkVersion, m_snippetType, getSettings());
            m_snippetFileDirty = true;
        }
    }

    /**
     * Completely reinitialises the internal {@link GuardedDocument} from the given settings, overwriting
     * any changes that may or may not have accumulated in the {@link GuardedDocument}.
     * @param sparkVersion the possibly new Spark version
     * @param settings the new {@link JavaSnippetSettings}
     * @param snippetType the {@link SnippetType}
     */
    public void updateDocumentFromSettings(final SparkVersion sparkVersion, final SnippetType snippetType,
        final JavaSnippetSettings settings) {
        if (!m_helper.supportSpark(sparkVersion)) {
            //update the helper if the spark version has changed
            m_helper = m_helperRegistry.getHelper(sparkVersion);
            //invalidate the class path as well when the Spark version changes
            invalidateClasspath();
        }
        m_snippetType = snippetType;
        m_settings = settings;
        m_helper.updateAllSections(m_snippetType, m_document, m_settings);
    }

    private void initializeSnippetDirectory() {

        try {
            m_snippetDir = FileUtil.createTempDir("knime_sparkjavasnippet");
        } catch (IOException ex) {
            NodeLogger.getLogger(getClass())
                .error("Could not create temporary directory for Java Snippet: " + ex.getMessage(), ex);
            // use the standard temp directory instead
            m_snippetDir = new File(KNIMEConstants.getKNIMETempDir());
        }

        m_snippetFile = new EclipseFileObject("SparkJavaSnippet",
            new File(m_snippetDir, m_helper.getSnippetClassName(m_snippetType) + ".java").toURI(), Kind.SOURCE,
            Charset.defaultCharset());

    }

    /**
     * Get compilation units used by the {@link JavaSnippetCompiler}
     *
     * @return the files to compile
     * @throws IOException When files cannot be created.
     */
    @Override
    public Iterable<? extends JavaFileObject> getCompilationUnits() throws IOException {

        if (m_snippetDir == null) {
            initializeSnippetDirectory();
        }

        if (m_snippetFileDirty) {
            writeDocToSnippetFile();
            m_snippetFileDirty = false;
        }

        return Collections.singletonList(m_snippetFile);
    }

    /**
     * Write the document out to the snippet file.
     */
    private void writeDocToSnippetFile() throws IOException {
        try (final Writer out = m_snippetFile.openWriter();) {
            try {
                Document doc = getDocument();
                out.write(doc.getText(0, doc.getLength()));
            } catch (BadLocationException e) {
                // this should never happen.
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Return true when this snippet is the creator and maintainer of the given source.
     *
     * @param source the source
     * @return if this snippet is the given source
     */
    @Override
    public boolean isSnippetSource(final JavaFileObject source) {
        return null != m_snippetFile ? source.equals(m_snippetFile) : false;
    }

    /**
     * Get the parser for the snippet's document.
     *
     * @return the parser
     */
    @Override
    public Parser getParser() {
        return m_parser;
    }

    /**
     * Create a template for this snippet.
     *
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
        if (m_snippetDir == null) {
            initializeSnippetDirectory();
        }

        return m_snippetDir;
    }

    /**
     * Get the jar files to be added to the class path.
     *
     * @return the jar files for the class path
     * @throws IOException when a file could not be loaded
     */
    @Override
    public File[] getClassPath() throws IOException {
        if (m_classpathCache == null) {
            List<File> baseJars = m_helper.getSnippetClasspath();
            File[] userJars = getUserJarFiles();

            m_classpathCache = new File[baseJars.size() + userJars.length];
            baseJars.toArray(m_classpathCache);

            int offset = baseJars.size();
            for (int i = 0; i < userJars.length; i++) {
                m_classpathCache[offset + i] = userJars[i];
            }
        }

        return m_classpathCache;
    }

    /**
     * @return Translated user jar file location (translates knime://...)
     */
    public File[] getUserJarFiles() {
        final String[] userJars = m_settings.getJarFiles();
        final File[] userJarFiles = new File[userJars.length];

        for (int i = 0; i < userJars.length; i++) {
            try {
                userJarFiles[i] =  FileUtil.getFileFromURL(new URL(userJars[i]));
            } catch (MalformedURLException mue) {
                userJarFiles[i] = new File(userJars[i]);
            }
        }

        return userJarFiles;
    }

    /**
     * Called by snippet dialog when the user changes the list of jar files.
     */
    public void invalidateClasspath() {
        m_classpathCache = null;
    }

    /**
     * Set the system fields in the java snippet (called by node dialog)
     *
     * @param fields the fields to set
     */
    @Override
    public void setJavaSnippetFields(final JavaSnippetFields fields) {
        m_settings.setJavaSnippetFields(fields);
        m_helper.updateGuardedSections(m_snippetType, m_document, fields);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finalize() throws Throwable {
        if (m_snippetDir != null) {
            FileUtil.deleteRecursively(m_snippetDir);
        }
        super.finalize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document getDocument() {
        return m_document;
    }

    /**
     * @return the name of the snippet class
     */
    public String getSnippetClassName() {
        return m_helper.getSnippetClassName(m_snippetType);
    }
}
