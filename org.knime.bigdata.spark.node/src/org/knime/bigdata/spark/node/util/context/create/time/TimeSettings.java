package org.knime.bigdata.spark.node.util.context.create.time;

import java.time.ZoneId;
import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.time.util.SettingsModelTimeZone;

/**
 * Settings class to describe time conversions between cluster and client.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class TimeSettings {

    /**
     * Described the time shift strategy.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public enum TimeShiftStrategy implements ButtonGroupEnumInterface {
            /**
             * No conversion.
             */
            NONE,

            /**
             * Use a given time zone.
             */
            FIXED,

            /**
             * Use default time zone from JVM running Spark driver.
             */
            DEFAULT_CLUSTER;

        @Override
        public String getText() {
            switch (this) {
                case NONE:
                    return "Do not set";
                case FIXED:
                    return "Use fixed time zone:";
                case DEFAULT_CLUSTER:
                    return "Use default time zone from Spark cluster";
                default:
                    throw new IllegalArgumentException("Unknown time shift strategy: " + toString());
            }
        }

        @Override
        public String getToolTip() {
            return getText();
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public boolean isDefault() {
            return this == NONE;
        }
    }

    private final SettingsModelString m_timeShiftStrategy =
        new SettingsModelString("timeShiftStrategy", TimeShiftStrategy.DEFAULT_CLUSTER.getActionCommand());

    private final SettingsModelTimeZone m_fixedTimeZone =
        new SettingsModelTimeZone("timeShiftFixedTimeZone", ZoneId.systemDefault());

    private final SettingsModelBoolean m_fixedTZFailOnDifferenClusterTZ =
        new SettingsModelBoolean("failOnDifferentFixedVsClusterTimeZone", true);

    /**
     * @return the time shift strategy model
     */
    protected SettingsModelString getTimeShiftStrategyModel() {
        return m_timeShiftStrategy;
    }

    /**
     * @return the time shift strategy
     */
    public TimeShiftStrategy getTimeShiftStrategy() {
        return TimeShiftStrategy.valueOf(m_timeShiftStrategy.getStringValue());
    }

    /**
     * @return the fixed time zone model
     */
    protected SettingsModelTimeZone getFixedTimeZoneModel() {
        return m_fixedTimeZone;
    }

    /**
     * @return the ID of the selected fixed time zone
     */
    public ZoneId getFixedTimeZoneId() {
        return m_fixedTimeZone.getZone();
    }

    /**
     * @return the fixedTZFailOnDifferenClusterTZ
     */
    public SettingsModelBoolean getFixedTZFailOnDifferenClusterTZModel() {
        return m_fixedTZFailOnDifferenClusterTZ;
    }


    /**
     * @return {@code true} if the node should fail on a different cluster default time zone
     */
    public boolean failOnDifferentClusterTZ() {
        if (getTimeShiftStrategy() == TimeShiftStrategy.FIXED) {
            return m_fixedTZFailOnDifferenClusterTZ.getBooleanValue();
        } else {
            return false;
        }
    }

    /**
     * Saves the current settings.
     *
     * @param settings where to save the settings into.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_timeShiftStrategy.saveSettingsTo(settings);
        m_fixedTimeZone.saveSettingsTo(settings);
        m_fixedTZFailOnDifferenClusterTZ.saveSettingsTo(settings);
    }

    /**
     * Validates the content of the given settings.
     *
     * @param settings The settings to validate.
     * @throws InvalidSettingsException if settings where invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_timeShiftStrategy.validateSettings(settings);
        m_fixedTimeZone.validateSettings(settings);
        m_fixedTZFailOnDifferenClusterTZ.validateSettings(settings);
    }

    /**
     * Validates the given settings into this instance.
     *
     * @param settings The settings to load.
     * @throws InvalidSettingsException if settings where invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_timeShiftStrategy.loadSettingsFrom(settings);
        m_fixedTimeZone.loadSettingsFrom(settings);
        m_fixedTZFailOnDifferenClusterTZ.loadSettingsFrom(settings);
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_fixedTimeZone.setEnabled(getTimeShiftStrategy() == TimeShiftStrategy.FIXED);
        m_fixedTZFailOnDifferenClusterTZ.setEnabled(getTimeShiftStrategy() == TimeShiftStrategy.FIXED);
    }

    /**
     * Add time zone settings to given spark settings.
     *
     * @param settings Settings map to add time zone settings.
     */
    public void addSparkSettings(final Map<String, String> settings) {
        if (getTimeShiftStrategy() == TimeShiftStrategy.FIXED) {
            settings.put("spark.sql.session.timeZone", m_fixedTimeZone.getZone().getId());
        }
    }

    public void loadPreKNIME4_2Default() {
        // for workflows that were created prior to KNIME 4.2 we will use the NONE timeshift strategy
        // in order to not change the behavior of existing workflows
        m_timeShiftStrategy.setStringValue(TimeShiftStrategy.NONE.getActionCommand());
    }

}
