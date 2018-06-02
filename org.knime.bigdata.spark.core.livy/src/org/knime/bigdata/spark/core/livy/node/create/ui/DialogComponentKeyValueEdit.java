package org.knime.bigdata.spark.core.livy.node.create.ui;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.port.PortObjectSpec;

/**
 * The dialog component allowing to edit key-value pairs.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class DialogComponentKeyValueEdit extends DialogComponent {

    /** The Key-value table panel. */
    private final KeyValueTablePanel m_tablePanel;
    
    private final KeyValueTableModel m_tableModel;

    /**
     * Constructor.
     *
     * @param settingsModel the Key-Value settings model
     */
    public DialogComponentKeyValueEdit(final SettingsModelKeyValue settingsModel) {
        super(settingsModel);
        
        // update the checkbox, whenever the model changes - make sure we get
        // notified first.
        settingsModel.prependChangeListener((e) -> updateComponent());
        
        m_tableModel = new KeyValueTableModel(settingsModel);
        m_tablePanel = new KeyValueTablePanel(m_tableModel);

        // add the panel
        getComponentPanel().setLayout(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;
        getComponentPanel().add(m_tablePanel, gbc);
        
        // call this method to be in sync with the settings model
        updateComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateComponent() {
        final SettingsModelKeyValue settingsModel = ((SettingsModelKeyValue)getModel());
        setEnabledComponents(settingsModel.isEnabled());
        m_tableModel.fireTableDataChanged();
        m_tablePanel.toggleButtons();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettingsBeforeSave() throws InvalidSettingsException {
        // inform panel that saving is about to take place
        m_tablePanel.onSave();
        // validate the model
        ((SettingsModelKeyValue)getModel()).validate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void checkConfigurabilityBeforeLoad(final PortObjectSpec[] specs) throws NotConfigurableException {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setEnabledComponents(final boolean enabled) {
        getComponentPanel().setEnabled(enabled);
        m_tablePanel.setEnabled(enabled);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setToolTipText(final String text) {
        getComponentPanel().setToolTipText(text);
    }
}