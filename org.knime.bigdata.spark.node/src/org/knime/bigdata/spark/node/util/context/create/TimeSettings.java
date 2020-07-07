package org.knime.bigdata.spark.node.util.context.create;

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
             * Use default time zone from JVM running KNIME.
             */
            DEFAULT_CLIENT,

            /**
             * Use default time zone from JVM running Spark driver.
             */
            DEFAULT_CLUSTER;

        @Override
        public String getText() {
            switch (this) {
                case NONE:
                    return "No time shift";
                case FIXED:
                    return "Fixed:";
                case DEFAULT_CLIENT:
                    return "Client: Use default time zone from KNIME (" + ZoneId.systemDefault() + ")";
                case DEFAULT_CLUSTER:
                    return "Cluster: Use default time zone from Spark driver";
                default:
                    throw new IllegalArgumentException("Unknown allocation strategy: " + toString());
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
        new SettingsModelString("timeShiftStrategy", TimeShiftStrategy.NONE.getActionCommand());

    private final SettingsModelTimeZone m_fixedTimeZone =
        new SettingsModelTimeZone("timeShiftFixedTimeZone", ZoneId.systemDefault());

    private final SettingsModelBoolean m_fixedTZFailOnDifferenClusterTZ =
        new SettingsModelBoolean("failOnDifferentFixedVsClusterTimeZone", true);

    private final SettingsModelBoolean m_clientTZFailOnDifferenClusterTZ =
        new SettingsModelBoolean("failOnDifferentClientVsClusterTimeZone", true);

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
     * @return the clientTZFailOnDifferenClusterTZ
     */
    public SettingsModelBoolean getClientTZFailOnDifferenClusterTZModel() {
        return m_clientTZFailOnDifferenClusterTZ;
    }

    /**
     * @return {@code true} if the node should fail on a different cluster default time zone
     */
    public boolean failOnDifferentClusterTZ() {
        if (getTimeShiftStrategy() == TimeShiftStrategy.FIXED) {
            return m_fixedTZFailOnDifferenClusterTZ.getBooleanValue();
        } else if (getTimeShiftStrategy() == TimeShiftStrategy.DEFAULT_CLIENT) {
            return m_clientTZFailOnDifferenClusterTZ.getBooleanValue();
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
        m_clientTZFailOnDifferenClusterTZ.saveSettingsTo(settings);
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
        m_clientTZFailOnDifferenClusterTZ.validateSettings(settings);
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
        m_clientTZFailOnDifferenClusterTZ.loadSettingsFrom(settings);
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_fixedTimeZone.setEnabled(getTimeShiftStrategy() == TimeShiftStrategy.FIXED);
        m_fixedTZFailOnDifferenClusterTZ.setEnabled(getTimeShiftStrategy() == TimeShiftStrategy.FIXED);
        m_clientTZFailOnDifferenClusterTZ.setEnabled(getTimeShiftStrategy() == TimeShiftStrategy.DEFAULT_CLIENT);
    }

    /**
     * Add time zone settings to given spark settings.
     *
     * @param settings Settings map to add time zone settings.
     */
    public void addSparkSettings(final Map<String, String> settings) {
        if (getTimeShiftStrategy() == TimeShiftStrategy.FIXED) {
            settings.put("spark.sql.session.timeZone", m_fixedTimeZone.getZone().getId());
        } else if (getTimeShiftStrategy() == TimeShiftStrategy.DEFAULT_CLIENT) {
            settings.put("spark.sql.session.timeZone", ZoneId.systemDefault().getId());
        }
    }

}
