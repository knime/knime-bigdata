/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 *   24.11.2011 (hofer): created
 */
package org.knime.bigdata.spark.node.scripting.java;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;
import javax.swing.event.ListDataEvent;
import javax.swing.event.ListDataListener;

import org.fife.rsta.ac.LanguageSupport;
import org.fife.rsta.ac.LanguageSupportFactory;
import org.fife.rsta.ac.java.JarManager;
import org.fife.rsta.ac.java.JavaLanguageSupport;
import org.fife.rsta.ac.java.buildpath.DirLibraryInfo;
import org.fife.ui.rsyntaxtextarea.ErrorStrip;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.folding.Fold;
import org.fife.ui.rsyntaxtextarea.folding.FoldManager;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
import org.knime.base.node.jsnippet.template.AddTemplateDialog;
import org.knime.base.node.jsnippet.template.DefaultTemplateController;
import org.knime.base.node.jsnippet.template.TemplateNodeDialog;
import org.knime.base.node.jsnippet.template.TemplateProvider;
import org.knime.base.node.jsnippet.template.TemplatesPanel;
import org.knime.base.node.jsnippet.ui.FieldsTableModel;
import org.knime.base.node.jsnippet.ui.FieldsTableModel.Column;
import org.knime.base.node.jsnippet.ui.FlowVariableList;
import org.knime.base.node.jsnippet.ui.InFieldsTable;
import org.knime.base.node.jsnippet.ui.JSnippetFieldsController;
import org.knime.base.node.jsnippet.ui.JSnippetTextArea;
import org.knime.base.node.jsnippet.ui.JarListPanel;
import org.knime.base.node.jsnippet.ui.OutFieldsTable;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.scripting.java.util.SparkJSnippet;
import org.knime.bigdata.spark.node.scripting.java.util.helper.AbstractJavaSnippetHelperRegistry;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import org.knime.bigdata.spark.node.scripting.java.util.template.JavaSnippetTemplateProviderRegistry;
import org.knime.bigdata.spark.node.scripting.java.util.template.SparkJavaSnippetTemplate;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ViewUtils;
import org.knime.core.node.workflow.FlowVariable;

/**
 * The dialog that is used in all Spark java snippet nodes.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippetNodeDialog extends NodeDialogPane implements TemplateNodeDialog<SparkJavaSnippetTemplate> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkJavaSnippetNodeDialog.class);

    /**
     * Title for tab with code templates
     */
    private static final String TITLE_TEMPLATES_TAB = "Templates";

    /**
     * Title for tab with additional libs (jar files)
     */
    private static final String TITLE_ADDITIONAL_LIBRARIES_TAB = "Additional Libraries";

    /**
     * Title for with the snippet code editor
     */
    private static final String TITLE_SNIPPET_TAB = "Java Snippet";

    /** The settings. */
    private final JavaSnippetSettings m_settings;

    /** Whether this dialog is a preview or not */
    private final boolean m_isPreview;

    /** The templates category for templates viewed or edited by this dialog. */
    @SuppressWarnings("rawtypes")
    protected final Class m_templateMetaCategory;

    private final SnippetType m_snippetType;

    private final AbstractJavaSnippetHelperRegistry m_helperRegistry;

    private SparkVersion m_sparkVersion;

    private SparkJSnippet m_snippet;

    private JSnippetFieldsController m_fieldsController;

    private SparkJavaSnippetNodeDialog m_previewPanel;

    private DefaultTemplateController<SparkJavaSnippetTemplate> m_templatesController;

    private TemplatesPanel<SparkJavaSnippetTemplate> m_templatesPanel;

    private File[] m_autoCompletionClassPath;

    /** GUI component with code editor */
    private JSnippetTextArea m_snippetTextArea;

    /** Component with a list of all input columns. */
    //    protected ColumnList m_colList;

    /** Component with a list of all input flow variables. */
    protected FlowVariableList m_flowVarsList;

    /** GUI component with input fields */
    private InFieldsTable m_inFieldsTable;

    /** GUI component with jar list */
    private JarListPanel m_jarPanel;

    private JLabel m_templateLocation;

    private boolean m_tabsInitialized;

    /**
     * Create a new Dialog
     *
     * @param templateMetaCategory the meta category used in the templates tab or to create templates
     * @param snippetType The type of the Spark java snippet (e.g. source)
     * @param helperRegistry
     */
    public SparkJavaSnippetNodeDialog(final Class<?> templateMetaCategory, final SnippetType snippetType,
        final AbstractJavaSnippetHelperRegistry helperRegistry) {
        m_templateMetaCategory = templateMetaCategory;
        m_snippetType = snippetType;
        m_helperRegistry = helperRegistry;
        m_isPreview = false;
        m_settings = new JavaSnippetSettings();
        m_tabsInitialized = false;

        // we delay the actual GUI initialization until loadSettingsFrom() is invoked, because we need
        // the Spark version of the current Spark context to create a SparkJavaSnippet instance
    }

    /**
     * Private constructor for code template previews
     *
     * @param templateMetaCategory
     * @param sparkVersion
     * @param snippetType
     * @param helperRegistry
     */
    private SparkJavaSnippetNodeDialog(final Class<?> templateMetaCategory, final SparkVersion sparkVersion,
        final SnippetType snippetType, final AbstractJavaSnippetHelperRegistry helperRegistry) {
        m_templateMetaCategory = templateMetaCategory;
        m_snippetType = snippetType;
        m_helperRegistry = helperRegistry;
        m_isPreview = true;
        m_settings = new JavaSnippetSettings();
        m_sparkVersion = sparkVersion;
        m_snippet = new SparkJSnippet(sparkVersion, snippetType, m_settings, m_helperRegistry);
        m_tabsInitialized = false;
        initTabs();
    }

    private void initTabs() {
        if (!m_tabsInitialized) {
            addTab(TITLE_SNIPPET_TAB, createSnippetsPanel());
            addTab(TITLE_ADDITIONAL_LIBRARIES_TAB, createJarPanel());
            if (!m_isPreview) {
                // The preview does not have the templates tab
                addTab(TITLE_TEMPLATES_TAB, createTemplatesPanel());
            }

            setEnabled(!m_isPreview);
            setSelected(TITLE_SNIPPET_TAB);
            m_tabsInitialized = true;
        }
    }

    private JPanel createSnippetsPanel() {
        JPanel snippetsPanel = new JPanel(new BorderLayout());
        JComponent snippet = createSnippetCodePanel();
        JComponent colsAndVars = createColsAndVarsPanel();

        JPanel centerPanel = new JPanel(new GridLayout(0, 1));
        JSplitPane centerSplitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        centerSplitPane.setLeftComponent(colsAndVars);
        centerSplitPane.setRightComponent(snippet);
        centerSplitPane.setResizeWeight(0.3); // colsAndVars expands to 0.3, the snippet to 0.7

        m_inFieldsTable = new InFieldsTable();
        m_inFieldsTable.setBorder(BorderFactory.createTitledBorder("Input"));

        OutFieldsTable dummyOutFieldsTable = createOutFieldsTable();
        //        m_outFieldsTable.setBorder(BorderFactory.createTitledBorder("Output"));

        // use split pane for fields
        JSplitPane fieldsPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        fieldsPane.setTopComponent(m_inFieldsTable);
        fieldsPane.setOneTouchExpandable(true);

        JSplitPane mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        mainSplitPane.setTopComponent(centerSplitPane);
        // minimize size of tables at the bottom
        fieldsPane.setPreferredSize(fieldsPane.getMinimumSize());
        mainSplitPane.setBottomComponent(fieldsPane);
        mainSplitPane.setOneTouchExpandable(true);
        mainSplitPane.setResizeWeight(0.7); // snippet gets more space, table with in/out gets less extra space

        centerPanel.add(mainSplitPane);

        snippetsPanel.add(centerPanel, BorderLayout.CENTER);
        JPanel templateInfoPanel = createTemplateInfoPanel();
        snippetsPanel.add(templateInfoPanel, BorderLayout.NORTH);
        JPanel optionsPanel = createOptionsPanel();
        if (optionsPanel != null) {
            snippetsPanel.add(optionsPanel, BorderLayout.SOUTH);
        }

        m_fieldsController = new JSnippetFieldsController(m_snippet, m_inFieldsTable, dummyOutFieldsTable);
        //        m_colList.install(m_snippetTextArea);
        //        m_colList.install(m_fieldsController);
        m_flowVarsList.install(m_snippetTextArea);
        m_flowVarsList.install(m_fieldsController);

        if (!m_isPreview) {
            snippetsPanel.setPreferredSize(new Dimension(800, 600));
        }

        return snippetsPanel;
    }

    /**
     * The panel at the top with the "Create Template..." Button.
     */
    private JPanel createTemplateInfoPanel() {
        final JButton addTemplateButton = new JButton("Create Template...");
        addTemplateButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                Frame parent = (Frame)SwingUtilities.getAncestorOfClass(Frame.class, addTemplateButton);
                SparkJavaSnippetTemplate newTemplate =
                    AddTemplateDialog.openUserDialog(parent, m_snippet, m_templateMetaCategory, getTemplateProvider());
                if (null != newTemplate) {
                    getTemplateProvider().addTemplate(newTemplate);
                    // update the template UUID of the current snippet
                    m_settings.setTemplateUUID(newTemplate.getUUID());
                    String loc = getTemplateProvider().getDisplayLocation(newTemplate);
                    m_templateLocation.setText(loc);
                    SparkJavaSnippetNodeDialog.this.getPanel().validate();
                }
            }
        });
        JPanel templateInfoPanel = new JPanel(new BorderLayout());
        TemplateProvider<SparkJavaSnippetTemplate> provider = getTemplateProvider();
        String uuid = m_settings.getTemplateUUID();
        SparkJavaSnippetTemplate template = null != uuid ? provider.getTemplate(UUID.fromString(uuid)) : null;
        String loc = null != template ? createTemplateLocationText(template) : "";
        m_templateLocation = new JLabel(loc);
        if (m_isPreview) {
            templateInfoPanel.add(m_templateLocation, BorderLayout.CENTER);
        } else {
            templateInfoPanel.add(addTemplateButton, BorderLayout.LINE_END);
        }
        templateInfoPanel.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
        return templateInfoPanel;
    }

    private JPanel createJarPanel() {
        m_jarPanel = new JarListPanel();
        m_jarPanel.addListDataListener(new ListDataListener() {
            private void updateSnippet() {
                m_settings.setJarFiles(m_jarPanel.getJarFiles());
                m_snippet.invalidateClasspath();
                // force reparsing of the snippet
                for (int i = 0; i < m_snippetTextArea.getParserCount(); i++) {
                    m_snippetTextArea.forceReparsing(i);
                }
                // update autocompletion
                updateAutocompletion();
            }

            @Override
            public void intervalRemoved(final ListDataEvent e) {
                updateSnippet();
            }

            @Override
            public void intervalAdded(final ListDataEvent e) {
                updateSnippet();
            }

            @Override
            public void contentsChanged(final ListDataEvent e) {
                updateSnippet();
            }
        });

        final JLabel warningLabel = new JLabel("The used libraries need to be present on your cluster and added to the class path of your Spark job server. They are not automatically uploaded!");
        final JPanel warningPanel = new JPanel(new FlowLayout()); // center label
        warningPanel.add(warningLabel);
        final JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.add(m_jarPanel, BorderLayout.CENTER);
        wrapper.add(warningPanel, BorderLayout.SOUTH);
        return wrapper;
    }

    /** Create the templates tab. */
    private JPanel createTemplatesPanel() {
        m_previewPanel = createPreview();
        m_templatesController = new DefaultTemplateController<>(this, m_previewPanel);
        m_templatesPanel = new TemplatesPanel<>(
            Collections.singleton(m_templateMetaCategory), m_templatesController, getTemplateProvider());
        return m_templatesPanel;
    }

    /**
     * Create a non editable preview to be used to display a template. This method is typically overridden by
     * subclasses.
     *
     * @return a new instance prepared to display a preview.
     */
    protected SparkJavaSnippetNodeDialog createPreview() {
        return new SparkJavaSnippetNodeDialog(m_templateMetaCategory, m_sparkVersion, m_snippetType, m_helperRegistry);
    }

    /**
     * Create table do display the ouput fields.
     *
     * @return the table
     */
    protected static OutFieldsTable createOutFieldsTable() {
        OutFieldsTable table = new OutFieldsTable(false);
        FieldsTableModel model = (FieldsTableModel)table.getTable().getModel();
        table.getTable().getColumnModel().getColumn(model.getIndex(Column.FIELD_TYPE)).setPreferredWidth(30);
        table.getTable().getColumnModel().getColumn(model.getIndex(Column.REPLACE_EXISTING)).setPreferredWidth(15);
        table.getTable().getColumnModel().getColumn(model.getIndex(Column.IS_COLLECTION)).setPreferredWidth(15);
        return table;
    }

    /**
     * Create the panel with the snippet.
     */
    private JComponent createSnippetCodePanel() {
        updateAutocompletion();

        m_snippetTextArea = new JSnippetTextArea(m_snippet);

        // reset style which causes a recreation of the folds
        // this code is also executed in "onOpen" but that is not called for the template viewer tab
        m_snippetTextArea.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_NONE);
        m_snippetTextArea.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JAVA);
        // collapse all folds
        int foldCount = m_snippetTextArea.getFoldManager().getFoldCount();
        for (int i = 0; i < foldCount; i++) {
            Fold fold = m_snippetTextArea.getFoldManager().getFold(i);
            fold.setCollapsed(true);
        }
        JScrollPane snippetScroller = new RTextScrollPane(m_snippetTextArea);
        JPanel snippet = new JPanel(new BorderLayout());
        snippet.add(snippetScroller, BorderLayout.CENTER);
        ErrorStrip es = new ErrorStrip(m_snippetTextArea);
        snippet.add(es, BorderLayout.LINE_END);
        return snippet;
    }

    /**
     * The panel at the left with the column and variables at the input. Override this method when the columns are
     * variables should not be displayed.
     *
     * @return the panel at the left with the column and variables at the input.
     */
    protected JComponent createColsAndVarsPanel() {
        JSplitPane varSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        //        m_colList = new ColumnList();

        //        JScrollPane colListScroller = new JScrollPane(m_colList);
        //        colListScroller.setBorder(createEmptyTitledBorder("Column List"));
        //        varSplitPane.setTopComponent(colListScroller);

        // set variable panel
        m_flowVarsList = new FlowVariableList();
        JScrollPane flowVarScroller = new JScrollPane(m_flowVarsList);
        flowVarScroller.setBorder(createEmptyTitledBorder("Flow Variable List"));
        varSplitPane.setBottomComponent(flowVarScroller);
        varSplitPane.setOneTouchExpandable(true);
        varSplitPane.setResizeWeight(0.9);

        return varSplitPane;
    }

    /**
     * Create Panel with additional options to be displayed in the south.
     *
     * @return options panel or null if there are no additional options.
     */
    protected JPanel createOptionsPanel() {
        return null;
    }

    private void updateAutocompletion() {
        LanguageSupportFactory lsf = LanguageSupportFactory.get();
        LanguageSupport support = lsf.getSupportFor(org.fife.ui.rsyntaxtextarea.SyntaxConstants.SYNTAX_STYLE_JAVA);
        JavaLanguageSupport jls = (JavaLanguageSupport)support;
        JarManager jarManager = jls.getJarManager();

        try {
            boolean doUpdate = false;
            if (filesExist(m_autoCompletionClassPath)) {
                m_autoCompletionClassPath = m_snippet.getClassPath();
                doUpdate = true;
            } else {
                if (!Arrays.equals(m_autoCompletionClassPath, m_snippet.getClassPath())) {
                    m_autoCompletionClassPath = m_snippet.getClassPath();
                    doUpdate = true;
                }
            }

            if (doUpdate) {
                jarManager.clearClassFileSources();
                jarManager.addCurrentJreClassFileSource();
                for (File classPathEntry : m_autoCompletionClassPath) {
                    if (classPathEntry.isFile()) {
                        jarManager.addClassFileSource(classPathEntry);
                    } else {
                        jarManager.addClassFileSource(new DirLibraryInfo(classPathEntry));
                    }
                }
            }

        } catch (IOException ioe) {
            LOGGER.error(ioe.getMessage(), ioe);
        }

    }

    /**
     * Tests if files in the given array exist.
     *
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
     * Create an empty, titled border.
     *
     * @param string Title of the border.
     * @return Such a new border.
     */
    protected Border createEmptyTitledBorder(final String string) {
        return BorderFactory.createTitledBorder(BorderFactory.createEmptyBorder(5, 0, 0, 0), string,
            TitledBorder.DEFAULT_JUSTIFICATION, TitledBorder.BELOW_TOP);
    }

    /**
     * Determines whether this component is enabled. An enabled component can respond to user input and generate events.
     *
     * @return <code>true</code> if the component is enabled, <code>false</code> otherwise
     */
    public boolean isEnabled() {
        return !m_isPreview;
    }

    /**
     * Sets whether or not this component is enabled. A component that is enabled may respond to user input, while a
     * component that is not enabled cannot respond to user input.
     *
     * @param enabled true if this component should be enabled, false otherwise
     */
    private void setEnabled(final boolean enabled) {
        m_flowVarsList.setEnabled(enabled);
        m_inFieldsTable.setEnabled(enabled);
        m_jarPanel.setEnabled(enabled);
        m_snippetTextArea.setEnabled(enabled);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean closeOnESC() {
        // do not close on ESC, since ESC is used to close autocomplete popups
        // in the snippets textarea.
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            ViewUtils.invokeAndWaitInEDT(new Runnable() {
                @Override
                public void run() {
                    try {
                        loadSettingsFromInternal(settings, specs);
                    } catch(NotConfigurableException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            });
        } catch(RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof NotConfigurableException) {
                throw ((NotConfigurableException) e.getCause());
            } else if (e.getCause() != null && e.getCause() instanceof InvalidSettingsException) {
                throw new NotConfigurableException(e.getCause().getMessage(), e.getCause());
            } else {
                throw e;
            }
        }
    }

    /**
     * Load settings invoked from the EDT-Thread.
     *
     * @param settings the settings to load
     * @param specs the specs of the input table
     * @throws NotConfigurableException on invalid settings
     */
    protected void loadSettingsFromInternal(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {

        m_settings.loadSettingsForDialog(settings);

        final DataTableSpec tableSpec;
        final SparkVersion lastSparkVersion = m_sparkVersion;
        if (specs == null || specs.length < 1 || specs[0] == null) {
            tableSpec = new DataTableSpec();
            m_sparkVersion = KNIMEConfigContainer.getSparkVersion();
        } else {
            m_sparkVersion = SparkContextUtil.getSparkVersion(((SparkContextProvider)specs[0]).getContextID());

            if (specs[0] instanceof SparkContextPortObjectSpec) {
                // this is a Spark Java Snipppet Source node which does not have an input table spec
                tableSpec = new DataTableSpec();
            } else {
                SparkDataPortObjectSpec rddSpec = (SparkDataPortObjectSpec)specs[0];
                tableSpec = rddSpec.getTableSpec();
            }
        }

        if (!m_helperRegistry.supportsVersion(m_sparkVersion)) {
            throw new NotConfigurableException("Unsupported Spark version: " + m_sparkVersion);
        }

        if (m_snippet == null) {
            m_snippet = new SparkJSnippet(m_sparkVersion, m_snippetType, m_settings, m_helperRegistry);
        } else {
            m_snippet.updateDocumentFromSettings(m_sparkVersion, m_snippetType, m_settings);
        }

        initTabs();

        //        m_colList.setSpec(specs[0]);
        m_flowVarsList.setFlowVariables(getAvailableFlowVariables().values());
        m_jarPanel.setJarFiles(m_settings.getJarFiles());
        m_fieldsController.updateData(m_settings, tableSpec, getAvailableFlowVariables());

        // set caret position to the start of the custom expression
        m_snippetTextArea.setCaretPosition(((GuardedDocument)m_snippet.getDocument())
            .getGuardedSection(JavaSnippetDocument.GUARDED_BODY_START).getEnd().getOffset() + 1);
        m_snippetTextArea.requestFocusInWindow();

        m_templatesController.setDataTableSpec(tableSpec);
        m_templatesController.setFlowVariables(getAvailableFlowVariables());

        // update template info panel
        TemplateProvider<SparkJavaSnippetTemplate> provider = getTemplateProvider();
        String uuid = m_settings.getTemplateUUID();
        SparkJavaSnippetTemplate template = null != uuid ? provider.getTemplate(UUID.fromString(uuid)) : null;
        String loc = null != template ? createTemplateLocationText(template) : "";
        m_templateLocation.setText(loc);

        // forward version changes to template and template preview panel
        if (lastSparkVersion != null && !lastSparkVersion.equals(m_sparkVersion) && !m_isPreview) {
            m_templatesPanel.setTemplateProvider(provider);
            m_previewPanel.setSparkVersion(m_sparkVersion);
        }
    }

    /** Update spark version in underlying snippet. */
    private void setSparkVersion(final SparkVersion sparkVersion) {
        if (m_snippet != null) {
            m_snippet.setSparkVersion(sparkVersion);
        }
    }

    /**
     * Reinitialize with the given blueprint.
     *
     * @param template the template
     * @param flowVariables the flow variables at the input
     * @param spec the input spec
     */
    @Override
    public void applyTemplate(final SparkJavaSnippetTemplate template, final DataTableSpec spec,
        final Map<String, FlowVariable> flowVariables) {

        // save and read settings to decouple objects.
        NodeSettings settings = new NodeSettings(template.getUUID());
        template.getSnippetSettings().saveSettings(settings);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            settings.saveToXML(os);
            NodeSettingsRO settingsro =
                NodeSettings.loadFromXML(new ByteArrayInputStream(os.toString("UTF-8").getBytes("UTF-8")));
            m_settings.loadSettings(settingsro);
            m_snippet.updateDocumentFromSettings(m_sparkVersion, m_snippetType, m_settings);

            //        m_colList.setSpec(spec);
            m_flowVarsList.setFlowVariables(flowVariables.values());

            m_jarPanel.setJarFiles(m_settings.getJarFiles());

            m_fieldsController.updateData(m_settings, spec, flowVariables);
            // update template info panel
            m_templateLocation.setText(createTemplateLocationText(template));

            setSelected(TITLE_SNIPPET_TAB);
            // set caret position to the start of the custom expression
            m_snippetTextArea.setCaretPosition(((GuardedDocument)m_snippet.getDocument())
                .getGuardedSection(JavaSnippetDocument.GUARDED_BODY_START).getEnd().getOffset() + 1);
            m_snippetTextArea.requestFocus();

        } catch (Exception e) {
            LOGGER.error("Cannot apply template.", e);
        }
    }

    /**
     * Get the template's location for display.
     *
     * @param template the template
     * @return the template's loacation for display
     */
    private String createTemplateLocationText(final SparkJavaSnippetTemplate template) {
        TemplateProvider<SparkJavaSnippetTemplate> provider = getTemplateProvider();
        return provider.getDisplayLocation(template);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onOpen() {
        m_snippetTextArea.requestFocus();
        m_snippetTextArea.requestFocusInWindow();
        // reset style which causes a recreation of the popup window with
        // the side effect, that all folds are recreated, so that we must collapse
        // them next (bug 4061)
        m_snippetTextArea.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_NONE);
        m_snippetTextArea.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JAVA);
        // collapse all folds
        FoldManager foldManager = m_snippetTextArea.getFoldManager();
        int foldCount = foldManager.getFoldCount();
        for (int i = 0; i < foldCount; i++) {
            Fold fold = foldManager.getFold(i);
            fold.setCollapsed(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        ViewUtils.invokeAndWaitInEDT(new Runnable() {

            @Override
            public void run() {
                // Commit editing - This is a workaround for a bug in the Dialog
                // since the tables do not loose focus when OK or Apply is
                // pressed.
                if (null != m_inFieldsTable.getTable().getCellEditor()) {
                    m_inFieldsTable.getTable().getCellEditor().stopCellEditing();
                }
                //                if (null != m_outFieldsTable.getTable().getCellEditor()) {
                //                    m_outFieldsTable.getTable().getCellEditor().
                //                    stopCellEditing();
                //                }
            }
        });
        JavaSnippetSettings s = m_snippet.getSettings();

        // if settings have less fields than defined in the table it means
        // that the tables contain errors
        FieldsTableModel inFieldsModel = (FieldsTableModel)m_inFieldsTable.getTable().getModel();
        if (!inFieldsModel.validateValues()) {
            throw new IllegalArgumentException("The input fields table has errors.");
        }
        //        FieldsTableModel outFieldsModel =
        //            (FieldsTableModel)m_outFieldsTable.getTable().getModel();
        //        if (!outFieldsModel.validateValues()) {
        //            throw new IllegalArgumentException(
        //                    "The output fields table has errors.");
        //        }
        // give subclasses the chance to modify settings
        preSaveSettings(s);

        s.saveSettings(settings);
    }

    /**
     * Called right before storing the settings object. Gives subclasses the chance to modify the settings object.
     *
     * @param s the settings
     */
    protected void preSaveSettings(final JavaSnippetSettings s) {
        // just a place holder.
    }

    /**
     * @return
     */
    private TemplateProvider<SparkJavaSnippetTemplate> getTemplateProvider() {
        return JavaSnippetTemplateProviderRegistry.getInstance().getTemplateProvider(m_sparkVersion);
    }
}
