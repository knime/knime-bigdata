package org.knime.bigdata.spark.core.livy.node.create;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class to describe what can be configured for a YARN container.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class ContainerResourceSettings {

    private final SettingsModelBoolean m_overrideDefault = new SettingsModelBoolean("overrideDefault", true);

    private final SettingsModelIntegerBounded m_memory = new SettingsModelIntegerBounded("memory", 1, 1, 100000);

    private final SettingsModelString m_memoryUnit = new SettingsModelString("memoryUnit", MemoryUnit.GB.toString());

    private final SettingsModelIntegerBounded m_cores = new SettingsModelIntegerBounded("cores", 1, 1, 100000);

    /**
     * @return settings model for whether to override default resource settings.
     */
    protected SettingsModelBoolean getOverrideDefaultModel() {
        return m_overrideDefault;
    }

    /**
     * 
     * @return whether to override default resource settings.
     */
    public boolean overrideDefault() {
        return m_overrideDefault.getBooleanValue();
    }

    /**
     * 
     * @return settings model for the amount of memory to request.
     */
    protected SettingsModelIntegerBounded getMemoryModel() {
        return m_memory;
    }

    /***
     * 
     * @return the amount of memory to request (unit-less number).
     */
    protected int getMemory() {
        return m_memory.getIntValue();
    }

    /**
     * 
     * @return settings model for the unit to use when interpreting the amount of memory to request.
     */
    protected SettingsModelString getMemoryUnitModel() {
        return m_memoryUnit;
    }

    /**
     * 
     * @return the unit to use when interpreting the amount of memory to request.
     */
    protected MemoryUnit getMemoryUnit() {
        return MemoryUnit.valueOf(m_memoryUnit.getStringValue());
    }

    /**
     * 
     * @return settings model for the amount of cores to request.
     */
    protected SettingsModelIntegerBounded getCoresModel() {
        return m_cores;
    }

    /**
     * 
     * @return the amount of cores to request.
     */
    public int getCores() {
        return m_cores.getIntValue();
    }

    /**
     * Saves the current settings.
     * 
     * @param settings where to save the settings into.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_overrideDefault.saveSettingsTo(settings);
        m_memory.saveSettingsTo(settings);
        m_memoryUnit.saveSettingsTo(settings);
        m_cores.saveSettingsTo(settings);
    }

    /**
     * Validates the content of the given settings.
     * 
     * @param settings The settings to validate.
     * @throws InvalidSettingsException if settings where invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overrideDefault.validateSettings(settings);
        m_memory.validateSettings(settings);
        m_memoryUnit.validateSettings(settings);
        m_cores.validateSettings(settings);
    }

    /**
     * Validates the given settings into this instance.
     * 
     * @param settings The settings to load.
     * @throws InvalidSettingsException if settings where invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overrideDefault.loadSettingsFrom(settings);
        m_memory.loadSettingsFrom(settings);
        m_memoryUnit.loadSettingsFrom(settings);
        m_cores.loadSettingsFrom(settings);
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_memory.setEnabled(m_overrideDefault.getBooleanValue());
        m_memoryUnit.setEnabled(m_overrideDefault.getBooleanValue());
        m_cores.setEnabled(m_overrideDefault.getBooleanValue());
    }
}
