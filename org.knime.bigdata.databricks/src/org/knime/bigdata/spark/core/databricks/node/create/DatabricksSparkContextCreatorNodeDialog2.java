/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.core.databricks.node.create;

import static java.util.Locale.ENGLISH;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.databricks.node.DbfsAuthenticationDialog;
import org.knime.bigdata.spark.core.databricks.DatabricksSparkContextProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.FileSystemBrowser.DialogType;
import org.knime.core.node.util.FileSystemBrowser.FileSelectionMode;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.util.Pair;
import org.knime.database.VariableContext;
import org.knime.database.agent.DBAgentFactory;
import org.knime.database.agent.DBAgentRegistry;
import org.knime.database.attribute.Attribute;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.component.attribute.AttributeCollectionBinding;
import org.knime.database.node.component.attribute.AttributeTablePanel;
import org.knime.database.node.component.attribute.DisplayedAttributeRepository;
import org.knime.database.node.connector.ConfigurationPanel;
import org.knime.database.node.connector.DBConnectorUIHelper;
import org.knime.database.node.connector.domain.DBDialectUI;
import org.knime.database.node.connector.domain.DBDriverUI;
import org.knime.database.node.connector.domain.DBTypeUI;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.ui.WorkingDirectoryChooser;
import org.knime.filehandling.core.defaultnodesettings.fileselection.FileSelectionDialog;
import org.knime.node.datatype.mapping.DialogComponentDataTypeMapping;

/**
 * Node dialog of the "Create Spark Context (Databricks)" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeDialog2 extends NodeDialogPane implements ChangeListener {

    private static final DBTypeUI DB_TYPE = new DBTypeUI(Databricks.DB_TYPE);

    private class NodeDialogVariableContext implements VariableContext {

        @Override
        public ICredentials getCredentials(final String id) {
            return getCredentialsProvider().get(id);
        }

        @Override
        public Collection<String> getCredentialsIds() {
            return getCredentialsProvider().listNames();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableFlowVariables();
        }

    }

    private static final String WORKING_DIR_HISTORY_ID = "databricks.workingDir";

    private static final String STAGING_AREA_DIR_HISTORY_ID = "databricks.stagingArea";

    private static final String JDBC_TAB = "DB Port";

    private static final String JDBC_TAB_CONNECTION = "Driver";

    private static final String JDBC_TAB_JDBC_PARAMETERS = "JDBC Parameters";

    private static final String JDBC_TAB_ADVANCED = "Advanced";

    private static final String ID_JDBC_PROPERTIES_ATTRIBUTE =
        DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES.getId();

    private static final Predicate<Attribute<?>> PREDICATE_JDBC_PROPERTIES_ATTRIBUTE =
        attribute -> attribute != null && ID_JDBC_PROPERTIES_ATTRIBUTE.equals(attribute.getId());

    private static final Predicate<Attribute<?>> PREDICATE_NOT_JDBC_PROPERTIES_ATTRIBUTE =
        PREDICATE_JDBC_PROPERTIES_ATTRIBUTE.negate();

    private final DatabricksSparkContextCreatorNodeSettings2 m_settings =
        new DatabricksSparkContextCreatorNodeSettings2();

    private final DialogComponentStringSelection m_sparkVersion = new DialogComponentStringSelection(
        m_settings.getSparkVersionModel(), null, new DatabricksSparkContextProvider().getSupportedSparkVersions()
            .stream().map(SparkVersion::getLabel).toArray(String[]::new));

    private final DialogComponentString m_databricksUrl =
        new DialogComponentString(m_settings.getDatabricksInstanceURLModel(), null, true, 40);

    private final DialogComponentString m_clusterId =
        new DialogComponentString(m_settings.getClusterIdModel(), null, true, 40);

    private final DialogComponentString m_workspaceId =
        new DialogComponentString(m_settings.getWorkspaceIdModel(), null, false, 40);

    private DbfsAuthenticationDialog m_authPanel;

    private final ChangeListener m_workingDirListener = new ChangeListener() {
        @Override
        public void stateChanged(final ChangeEvent e) {
            updateWorkingDirSetting();
        }
    };

    private final WorkingDirectoryChooser m_workingDirChooser =
        new WorkingDirectoryChooser(WORKING_DIR_HISTORY_ID, this::createFSConnection);

    private final DialogComponentBoolean m_createSparkContext = new DialogComponentBoolean(
        m_settings.getCreateSparkContextModel(), "Create Spark context and enable Spark context port");

    private final DialogComponentBoolean m_setStagingAreaFolder =
        new DialogComponentBoolean(m_settings.getSetStagingAreaFolderModel(), "Set staging area for Spark jobs");

    private FileSelectionDialog m_stagingAreaFileSelection;

    private final DialogComponentNumber m_connectionTimeout =
        new DialogComponentNumber(m_settings.getConnectionTimeoutModel(), null, 10, 3);

    private final DialogComponentNumber m_receiveTimeout =
        new DialogComponentNumber(m_settings.getReceiveTimeoutModel(), null, 10, 3);

    private final DialogComponentNumber m_jobCheckFrequency = new DialogComponentNumber(
        m_settings.getJobCheckFrequencyModel(), null, 1, 3);

    private final DialogComponentBoolean m_terminateClusterOnDestroy =
            new DialogComponentBoolean(m_settings.getTerminateClusterOnDestroyModel(), "Terminate cluster on context destroy");

    private final List<DialogComponent> m_dialogComponents = new ArrayList<>();

    private final JTabbedPane m_jdbcTabPanel;

    private final ConfigurationPanel<DatabricksSparkContextCreatorNodeSettings2> m_jdbcConfigurationPanel;

    private final AttributeTablePanel m_jdbcParametersPanel;

    private final AttributeTablePanel m_jdbcAdvancedPanel;

    private final DialogComponentDataTypeMapping<SQLType> m_externalToKnimeTypeMappingComponent;

    private final DialogComponentDataTypeMapping<SQLType> m_knimeToExternalTypeMappingComponent;

    private final DisplayedAttributeRepository m_attributeRepository = new DisplayedAttributeRepository();

    private final AttributeCollectionBinding<Map<Class<?>, DBAgentFactory>> m_agentAttributesBinding;

    private final AttributeCollectionBinding<DBSQLDialectFactory> m_dialectAttributesBinding;

    private final AttributeCollectionBinding<DBDriverWrapper> m_driverAttributesBinding;

    private final VariableContext m_variableContext = new NodeDialogVariableContext();

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeDialog2() {
        m_authPanel = new DbfsAuthenticationDialog(m_settings.getAuthenticationSettings(), this);

        addTab("Settings", createSettingsTab());
        addTab("Advanced", createAdvancedTab());

        m_jdbcTabPanel = new JTabbedPane(SwingConstants.TOP);
        addTab(JDBC_TAB, m_jdbcTabPanel, false);

        m_jdbcConfigurationPanel = new ConfigurationPanel<>(" Configuration ", () -> new DBTypeUI[]{DB_TYPE},
                DBConnectorUIHelper::getNonDefaultDBDialects, DBConnectorUIHelper::getDBDrivers);
        final JPanel jdbcConfContainer = new JPanel(new BorderLayout());
        jdbcConfContainer.add(m_jdbcConfigurationPanel, BorderLayout.NORTH);
        m_jdbcTabPanel.addTab(JDBC_TAB_CONNECTION, jdbcConfContainer);

        // Create attribute bindings.
        m_agentAttributesBinding = m_attributeRepository.bind(this::getAgentFactories,
            factories -> factories == null ? null
                : factories.entrySet().stream().map(entry -> entry.getValue().getAttributes(entry.getKey()))
                    .collect(Collectors.toCollection(() -> new ArrayList<>(factories.size()))));

        m_dialectAttributesBinding = m_attributeRepository.bind(this::getSelectedDialect,
            factory -> factory == null ? null : Collections.singleton(factory.getAttributes()));

        m_driverAttributesBinding = m_attributeRepository.bind(this::getSelectedDriver,
            driver -> driver == null ? null : Collections.singleton(driver.getAttributes()));

        m_jdbcConfigurationPanel.addDialectActionListener(event -> m_dialectAttributesBinding.update());
        m_jdbcConfigurationPanel.addDriverActionListener(event -> m_driverAttributesBinding.update());

        m_jdbcParametersPanel = createJdbcParametersPanel();
        final Dimension preferredSize = new Dimension(600, 300);
        m_jdbcParametersPanel.setPreferredSize(preferredSize);
        m_jdbcTabPanel.addTab(JDBC_TAB_JDBC_PARAMETERS, m_jdbcParametersPanel);

        m_jdbcAdvancedPanel = createAdvancedPanel();
        m_jdbcAdvancedPanel.setPreferredSize(preferredSize);
        m_jdbcTabPanel.addTab(JDBC_TAB_ADVANCED, m_jdbcAdvancedPanel);

        m_externalToKnimeTypeMappingComponent =
            new DialogComponentDataTypeMapping<>(m_settings.getExternalToKnimeMappingModel(), true);
        m_dialogComponents.add(m_externalToKnimeTypeMappingComponent);
        m_jdbcTabPanel.addTab(DialogComponentDataTypeMapping.TAB_EXTERNAL_TO_KNIME,
            m_externalToKnimeTypeMappingComponent.getComponentPanel());

        m_knimeToExternalTypeMappingComponent =
            new DialogComponentDataTypeMapping<>(m_settings.getKnimeToExternalMappingModel(), true);
        m_dialogComponents.add(m_knimeToExternalTypeMappingComponent);
        m_jdbcTabPanel.addTab(DialogComponentDataTypeMapping.TAB_KNIME_TO_EXTERNAL,
            m_knimeToExternalTypeMappingComponent.getComponentPanel());

        refreshDataTypeMappingComponents();
    }

    private JPanel createAdvancedTab() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);
        panel.add(createSparkContextPanel());
        panel.add(createTimeoutPanel());
        return panel;
    }

    private JPanel createSparkContextPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Spark context"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 1;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 0, 5, 5);
        addDialogComponentToPanel(m_createSparkContext, panel, gbc);
        m_createSparkContext.getModel().addChangeListener(this);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_setStagingAreaFolder, panel, gbc);
        m_setStagingAreaFolder.getModel().addChangeListener(this);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 25, 5, 20);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        m_stagingAreaFileSelection = new FileSelectionDialog( //
            STAGING_AREA_DIR_HISTORY_ID, //
            25, // history length
            () -> null, //
            DialogType.SAVE_DIALOG, //
            FileSelectionMode.DIRECTORIES_ONLY, //
            new String[0], //
            e -> {
            });
        panel.add(m_stagingAreaFileSelection.getPanel(), gbc);
        gbc.fill = GridBagConstraints.NONE;

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 0, 5, 5);
        addDialogComponentToPanel(m_terminateClusterOnDestroy, panel, gbc);

        return panel;
    }

    private JPanel createTimeoutPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Timeouts and update frequency"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        addDialogComponentToPanel("Databricks connection timeout (seconds):", m_connectionTimeout, false, panel, gbc);
        addDialogComponentToPanel("Databricks receive timeout (seconds):", m_receiveTimeout, false, panel, gbc);
        addDialogComponentToPanel("Job status polling interval (seconds):", m_jobCheckFrequency, false, panel, gbc);
        return panel;
    }

    private JPanel createSettingsTab() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);
        panel.add(createConnectionPanel());
        panel.add(createAuthPanel());
        panel.add(createFileSystemSettingsPanel());
        return panel;
    }

    private JPanel createConnectionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Connection settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;

        addDialogComponentToPanel("Spark version:", m_sparkVersion, true, panel, gbc);
        addDialogComponentToPanel("Databricks URL:", m_databricksUrl, false, panel, gbc);
        addDialogComponentToPanel("Cluster ID:", m_clusterId, false, panel, gbc);
        addDialogComponentToPanel("Workspace ID:", m_workspaceId, false, panel, gbc);

        // listen for updates to change DBFS connection for staging area browsing
        m_databricksUrl.getModel().addChangeListener(this);

        return panel;
    }

    private JComponent createAuthPanel() {
        final JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createTitledBorder("Authentication"));
        panel.add(m_authPanel);
        return panel;
    }

    private Component createFileSystemSettingsPanel() {
        final JPanel panel = new JPanel();
        final BoxLayout parentLayout = new BoxLayout(panel, BoxLayout.Y_AXIS);
        panel.setLayout(parentLayout);
        panel.add(Box.createHorizontalStrut(5));
        panel.setBorder(createTitledBorder("File System settings"));
        panel.add(m_workingDirChooser);
        return panel;
    }

    private static Border createTitledBorder(final String title) {
        return new TitledBorder(new EtchedBorder(EtchedBorder.RAISED), title);
    }

    private void addDialogComponentToPanel(final DialogComponent comp, final JPanel panel, final GridBagConstraints gbc) {
        m_dialogComponents.add(comp);
        panel.add(comp.getComponentPanel(), gbc);
    }

    private void addDialogComponentToPanel(final String label, final DialogComponent comp, final boolean smallComponent,
        final JPanel panel,
        final GridBagConstraints gbc) {
        m_dialogComponents.add(comp);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.EAST;
        gbc.weightx = 0;
        gbc.insets = new Insets(0, 5, 0, 0);
        panel.add(new JLabel(label), gbc);

        gbc.insets = new Insets(0, 0, 0, 0);

        if (smallComponent) { // add second column
            gbc.gridx++;
            gbc.anchor = GridBagConstraints.WEST;
            panel.add(comp.getComponentPanel(), gbc);
            gbc.gridx++;
            panel.add(new JLabel(), gbc);
        } else { // use two columns
            gbc.gridx++;
            gbc.anchor = GridBagConstraints.WEST;
            gbc.gridwidth = 2;
            panel.add(comp.getComponentPanel(), gbc);
        }

        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.gridwidth = 1;
        panel.add(new JLabel(), gbc);
    }

    private static HashMap<AuthenticationType, Pair<String, String>> getAuthLabelMap() {
        final HashMap<AuthenticationType, Pair<String, String>> map = new HashMap<>();
        map.put(AuthenticationType.PWD, new Pair<>("Token", "Token based authentication. No username required."));
        return map;
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        m_settings.updateEnabledness();

        final Object s = e.getSource();
        if (s.equals(m_settings.getSetStagingAreaFolderModel())) {
            m_stagingAreaFileSelection.setEnabled(m_settings.isStagingAreaFolderSet());
        }
    }

    //********************** Attribute Bindings **********************

    private Map<Class<?>, DBAgentFactory> getAgentFactories() {
        return DBAgentRegistry.getInstance().getFactories(Databricks.DB_TYPE);
    }

    private DBSQLDialectFactory getSelectedDialect() {
        final DBDialectUI dialectUI = m_jdbcConfigurationPanel.getDialect();
        return dialectUI == null ? null : dialectUI.getDialectFactory();
    }

    private DBDriverWrapper getSelectedDriver() {
        final DBDriverUI driverUI = m_jdbcConfigurationPanel.getDriver();
        return driverUI == null ? null : driverUI.getDriver();
    }

    //********************** Attribute Validation **********************

    /**
     * Gets an error message iff the second argument, a key from an associative value of an attribute identified by the
     * first argument, is erroneous.
     *
     * @param attributeId the ID of the attribute that contains the key.
     * @param key the key to be tested.
     * @return an error message iff {@code key} in the attribute identified by {@code attributeId} is erroneous, e.g.
     *         reserved.
     */
    protected String getAttributeEntryKeyErrorMessage(final String attributeId, final String key) {
        final DBDriverWrapper driver = getSelectedDriver();
        if (driver == null) {
            return null;
        }
        final Set<String> lowerCaseReservedKeys = DBDriverRegistry.getInstance()
            .getLowerCaseReservedDriverAttributeEntryKeys(driver.getDriverDefinition().getId()).get(attributeId);
        return lowerCaseReservedKeys == null
            || !lowerCaseReservedKeys.contains(key == null ? null : key.toLowerCase(ENGLISH)) ? null
                : "<html>Reserved key.<p>This key value has been reserved"
                    + " in the KNIME database preferences file.</p></html>";
    }

   //********************** JDBC Parameters Tab **********************

    private AttributeTablePanel createJdbcParametersPanel() {
        return new AttributeTablePanel(PREDICATE_JDBC_PROPERTIES_ATTRIBUTE, this::getAttributeEntryKeyErrorMessage,
            m_variableContext, m_driverAttributesBinding);
    }

    //********************** Advanced Tab **********************

    private AttributeTablePanel createAdvancedPanel() {
        return new AttributeTablePanel(PREDICATE_NOT_JDBC_PROPERTIES_ATTRIBUTE, this::getAttributeEntryKeyErrorMessage,
            m_variableContext, m_agentAttributesBinding, m_dialectAttributesBinding, m_driverAttributesBinding);
    }

    //********************** Type mapping **********************

    private void refreshDataTypeMappingComponents() {
        final DBTypeMappingService<?, ?> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(Databricks.DB_TYPE);
        refreshDataTypeMapping(mappingService, m_externalToKnimeTypeMappingComponent,
            DataTypeMappingDirection.EXTERNAL_TO_KNIME);
        refreshDataTypeMapping(mappingService, m_knimeToExternalTypeMappingComponent,
            DataTypeMappingDirection.KNIME_TO_EXTERNAL);
    }

    private static void refreshDataTypeMapping(final DBTypeMappingService<?, ?> mappingService,
        final DialogComponentDataTypeMapping<SQLType> mappingComponent, final DataTypeMappingDirection direction) {

        final DataTypeMappingConfiguration<SQLType> emptyMappingConfiguration =
            mappingService.createMappingConfiguration(direction);
        final DataTypeMappingConfiguration<SQLType> defaultMappingConfiguration =
            mappingService.createDefaultMappingConfiguration(direction);
        mappingComponent.setMappingService(mappingService);
        mappingComponent.setInputDataTypeMappingConfiguration(emptyMappingConfiguration);
        mappingComponent.getDataTypeMappingModel().setDataTypeMappingConfiguration(defaultMappingConfiguration);
        mappingComponent.updateComponent();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.getStagingAreaFolderModel().setStringValue(m_stagingAreaFileSelection.getSelected());
        updateDBSettings();
        m_settings.validateDeeper();
        m_authPanel.saveSettingsTo(settings);
        m_settings.saveSettingsForDialog(settings);
        m_workingDirChooser.addCurrentSelectionToHistory();
        m_stagingAreaFileSelection.addCurrentSelectionToHistory();

        // We need to do this to trigger validation in some of the dialog components and produce an error
        // when OK or Apply is clicked while invalid values have been entered.
        // Settings models such as SettingsModelString don't accept invalid values. Hence if someone
        // enters an invalid value in the dialog component it will not be put into the settings model.
        // Hence settings model and dialog component go out of sync. If we just save the settings model,
        // it will just save the previous valid value and the dialog will close  with out error
        for (final DialogComponent dialogComponent : m_dialogComponents) {
            dialogComponent.saveSettingsTo(settings);
        }
    }

    private void updateWorkingDirSetting() {
        m_settings.getWorkingDirectorySettingsModel().setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());
    }

    /**
     * Store DB settings from components into settings object.
     */
    private void updateDBSettings() throws InvalidSettingsException {
        m_jdbcConfigurationPanel.updateSettings(m_settings);

        // Non-short-circuiting.
        if (!m_jdbcParametersPanel.stopTableCellEditing() || !m_jdbcAdvancedPanel.stopTableCellEditing()) {
            // The panels may throw a more specific exception.
            m_jdbcParametersPanel.validateValues();
            m_jdbcAdvancedPanel.validateValues();
            throw new InvalidSettingsException("Not all attribute values are valid.");
        }
        m_settings.transferAttributeValuesIn(m_attributeRepository.getAllAttributes());
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        try {
            m_authPanel.loadSettingsFrom(settings, specs);
            m_settings.loadSettingsForDialog(settings);
            m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectory());
            m_workingDirChooser.addListener(m_workingDirListener);
            m_stagingAreaFileSelection.setSelected(m_settings.getStagingAreaFolder());
            m_stagingAreaFileSelection.setEnabled(m_settings.isStagingAreaFolderSet());
            updateDBComponents(specs);
        } catch (final InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }

    private FSConnection createFSConnection() throws IOException {
        try {
            return m_settings.createDatabricksFSConnection(getCredentialsProvider());
        } catch (InvalidSettingsException e) {
            throw new IOException("Unable to create DBFS connectin: " + e.getMessage(), e);
        }
    }

    @Override
    public void onOpen() {
        m_authPanel.onOpen();
    }

    @Override
    public void onClose() {
        m_workingDirChooser.removeListener(m_workingDirListener);
        m_workingDirChooser.onClose();
        m_stagingAreaFileSelection.onClose();
    }

    /**
     * Load DB settings from setting object into components.
     */
    private void updateDBComponents(final PortObjectSpec[] specs) throws NotConfigurableException {
        m_jdbcConfigurationPanel.updateComponent(m_settings, specs);

        // Attributes
        m_attributeRepository.refreshBindings();
        final Collection<Attribute<?>> attributes = m_attributeRepository.getAllAttributes();
        if (!attributes.isEmpty()) {
            m_settings.transferAttributeValuesOut(attributes);
        }
    }

}
