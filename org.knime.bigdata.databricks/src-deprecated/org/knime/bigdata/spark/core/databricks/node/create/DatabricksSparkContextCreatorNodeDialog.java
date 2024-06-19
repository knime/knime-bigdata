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
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
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

import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.spark.core.databricks.DatabricksSparkContextProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
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
import org.knime.node.datatype.mapping.DialogComponentDataTypeMapping;

/**
 * Node dialog of the "Create Spark Context (Databricks)" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeDialog extends NodeDialogPane implements ChangeListener {

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
        @Deprecated
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableFlowVariables();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables(final VariableType<?>[] types) {
            return getAvailableFlowVariables(types);
        }

    }

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

    private final DatabricksSparkContextCreatorNodeSettings m_settings = new DatabricksSparkContextCreatorNodeSettings();

    private final DialogComponentStringSelection m_sparkVersion = new DialogComponentStringSelection(
        m_settings.getSparkVersionModel(), "Spark version: ", new DatabricksSparkContextProvider().getSupportedSparkVersions()
            .stream().map(SparkVersion::getLabel).toArray(String[]::new));

    private final DialogComponentString m_databricksUrl =
            new DialogComponentString(m_settings.getDatabricksInstanceURLModel(), "Databricks URL:", true, 40);

    private final DialogComponentString m_clusterId =
            new DialogComponentString(m_settings.getClusterIdModel(), "Cluster ID:", true, 40);

    private final DialogComponentString m_workspaceId =
            new DialogComponentString(m_settings.getWorkspaceIdModel(), "Workspace ID:", false, 40);

    private final DialogComponentAuthentication m_authentication = new DialogComponentAuthentication(
        m_settings.getAuthenticationModel(), "Authentication", getAuthLabelMap(),
        AuthenticationType.USER_PWD, AuthenticationType.PWD, AuthenticationType.CREDENTIALS);

    private final DialogComponentBoolean m_createSparkContext =
        new DialogComponentBoolean(m_settings.getCreateSparkContextModel(), "Create Spark context");

    private final DialogComponentBoolean m_setStagingAreaFolder =
        new DialogComponentBoolean(m_settings.getSetStagingAreaFolderModel(), "Set staging area for Spark jobs");

    private RemoteFileChooserPanel m_stagingAreaFolder;

    private final DialogComponentNumber m_connectionTimeout =
        new DialogComponentNumber(m_settings.getConnectionTimeoutModel(), "Databricks connection timeout (seconds): ", 10, 3);

    private final DialogComponentNumber m_receiveTimeout =
        new DialogComponentNumber(m_settings.getReceiveTimeoutModel(), "Databricks receive timeout (seconds): ", 10, 3);

    private final DialogComponentNumber m_jobCheckFrequency = new DialogComponentNumber(
        m_settings.getJobCheckFrequencyModel(), "Job status polling interval (seconds): ", 1, 3);

    private final DialogComponentBoolean m_terminateClusterOnDestroy =
            new DialogComponentBoolean(m_settings.getTerminateClusterOnDestroyModel(), "Terminate cluster on context destroy");

    private final List<DialogComponent> m_dialogComponents = new ArrayList<>();

    private final JTabbedPane m_jdbcTabPanel;

    private final ConfigurationPanel<DatabricksSparkContextCreatorNodeSettings> m_jdbcConfigurationPanel;

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
    DatabricksSparkContextCreatorNodeDialog() {
        m_authentication.setPasswordOnlyLabel("Token:");
        addTab("General", createGeneralTab());
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
        final JPanel panel = new JPanel(new GridBagLayout());
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
        m_stagingAreaFolder = new RemoteFileChooserPanel(panel, "Staging area folder", false,
            "databricksStagingAreaFolderHistory", RemoteFileChooser.SELECT_DIR,
            createFlowVariableModel(m_settings.getStagingAreaFolderModel()), null);
        panel.add(m_stagingAreaFolder.getPanel(), gbc);
        m_settings.getStagingAreaFolderModel().addChangeListener(this);
        gbc.fill = GridBagConstraints.NONE;

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 0, 5, 5);
        addDialogComponentToPanel(m_terminateClusterOnDestroy, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);
        addDialogComponentToPanel(m_connectionTimeout, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_receiveTimeout, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_jobCheckFrequency, panel, gbc);

        return panel;
    }

    private JPanel createGeneralTab() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHEAST;
        gbc.weightx = 1;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        addDialogComponentToPanel(m_sparkVersion, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_databricksUrl, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_clusterId, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_workspaceId, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        addDialogComponentToPanel(m_authentication, panel, gbc);

        // listen for updates to change DBFS connection for staging area browsing
        m_databricksUrl.getModel().addChangeListener(this);
        m_authentication.getModel().addChangeListener(this);

        return panel;
    }

    private void addDialogComponentToPanel(final DialogComponent comp, final JPanel panel, final GridBagConstraints gbc) {
        m_dialogComponents.add(comp);
        panel.add(comp.getComponentPanel(), gbc);
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
            m_stagingAreaFolder.setEnabled(m_settings.isStagingAreaFolderSet());
        } else if (s.equals(m_settings.getDatabricksInstanceURLModel())
                || s.equals(m_settings.getAuthenticationModel())) {
            updateStagingAreaConnection();
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
        m_settings.getStagingAreaFolderModel().setStringValue(m_stagingAreaFolder.getSelection());
        updateDBSettings();
        m_settings.validateDeeper();
        m_settings.saveSettingsTo(settings);

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
            m_settings.loadSettingsFrom(settings);
            m_authentication.loadSettingsFrom(settings, specs, getCredentialsProvider());
            m_stagingAreaFolder.setSelection(m_settings.getStagingAreaFolder());
            m_stagingAreaFolder.setEnabled(m_settings.isStagingAreaFolderSet());

            updateDBComponents(specs);
            updateStagingAreaConnection();
        } catch (final InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
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

    private void updateStagingAreaConnection() {
        try {
            final CredentialsProvider cp = getCredentialsProvider();
            m_settings.validateDeeper();
            m_stagingAreaFolder.setConnectionInformation(m_settings.createDBFSConnectionInformation(cp));
        } catch (final InvalidSettingsException e) {
            m_stagingAreaFolder.setConnectionInformation(null);
        }
    }
}
