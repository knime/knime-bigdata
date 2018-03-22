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
 *   Created on 03.07.2015 by koetter
 *   Changes on 07.06.2016 by Sascha Wolke:
 *     - fields added: jobServerUrl, authentication, sparkJobLogLevel, overrideSparkSettings, customSparkSettings
 *     - protocol+host+port migrated into jobServerUrl
 *     - authentication flag added
 *     - deleteRDDsOnDispose renamed to deleteObjectsOnDispose
 *     - memPerNode migrated into overrideSparkSettings+customSparkSettings
 */
package org.knime.bigdata.spark.core.livy.node.create;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Settings class for the "Create Local Big Data Environment" node.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 * @see LivySparkContextCreatorNodeModel
 * @see LivySparkContextCreatorNodeDialog
 */
public class LivySparkContextSettings {

	/**
	 * Enum to model what happens when the KNIME workflow is closed.
	 * 
	 * @author Bjoern Lohrmann, KNIME GmbH
	 */
    public enum OnDisposeAction implements ButtonGroupEnumInterface {
		
		DESTROY_CTX,
		DELETE_DATA,
		DO_NOTHING;
	
		@Override
		public String getText() {
			switch(this) {
			case DESTROY_CTX:
				return "Destroy Spark context";
			case DELETE_DATA:
				return "Delete Spark DataFrames/RDDs";
			default:
				return "Do nothing";
			}
		}
	
		@Override
		public String getActionCommand() { return this.toString(); }
	
		@Override
		public String getToolTip() { return null; }
	
		@Override
		public boolean isDefault() { return this == DELETE_DATA; }
	}

	private static final String DEFAULT_CONTEXT_NAME = "knimeSparkContext";
    
    private static final String DEFAULT_CUSTOM_SPARK_SETTINGS = "spark.jars: /path/to/some.jar\n";

    // Spark context settings
    private final SettingsModelString m_livyUrl = new SettingsModelString("livyUrl", "http://localhost:8998/");
    
    private final SettingsModelString m_contextName = new SettingsModelString("contextName", DEFAULT_CONTEXT_NAME);

	private SettingsModelString m_onDisposeAction = new SettingsModelString("onDisposeAction",
			LivySparkContextSettings.OnDisposeAction.DELETE_DATA.getActionCommand());
    
    private final SettingsModelBoolean m_overrideSparkSettings =
        new SettingsModelBoolean("overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelString m_customSparkSettings =
        new SettingsModelString("customSparkSettings", DEFAULT_CUSTOM_SPARK_SETTINGS);

    private final SettingsModelBoolean m_hideExistsWarning =
            new SettingsModelBoolean("hideExistsWarning", false);
    
	/**
	 * This is a derived value that is set every when loading settings.
	 */
	private SparkContextID m_sparkContextID = LivySparkContextConfig.createSparkContextID(m_livyUrl.getStringValue(),
			m_contextName.getStringValue());

    /**
     * Constructor.
     */
    public LivySparkContextSettings() {
        m_customSparkSettings.setEnabled(m_overrideSparkSettings.getBooleanValue());
    }
    
    /**
     * @return the settings model for the Livy URL.
     * @see #getLivyUrl()
     */
    protected SettingsModelString getLivyUrlModel() {
        return m_livyUrl;
    }
    
    /**
     * 
     * @return the Livy URL.
     */
    public String getLivyUrl() {
    	return m_livyUrl.getStringValue();
    }


    /**
     * @return the settings model for the the context name.
     * @see #getContextName()
     */
    protected SettingsModelString getContextNameModel() {
        return m_contextName;
    }

    /**
     * @return a unique, human-readable name for the local Spark context.
     */
    public String getContextName() {
    	return m_contextName.getStringValue();
    }

    /**
     * 
     * @return the action to take when the KNIME workflow is closed.
     */
    public OnDisposeAction getOnDisposeAction() {
    	return OnDisposeAction.valueOf(m_onDisposeAction.getStringValue());
    }
    
	/**
	 * 
	 * @return settings model for the action to take when the KNIME workflow is
	 *         closed.
	 * @see #getOnDisposeAction()
	 */
    public SettingsModelString getOnDisposeActionModel() {
    	return m_onDisposeAction;
    }

    /**
     * 
     * @return settings model for whether to use custom spark settings or not.
     * @see #useCustomSparkSettings()
     */
    protected SettingsModelBoolean getUseCustomSparkSettingsModel() {
        return m_overrideSparkSettings;
    }

    /**
     * 
     * @return settings model that says which custom spark settings to use.
     * @see #getCustomSparkSettings()
     */
    protected SettingsModelString getCustomSparkSettingsModel() {
        return m_customSparkSettings;
    }
    
    /**
     * 
     * @return strings that contains the custom spark settings to use.
     */
    public String getCustomSparkSettingsString() {
        return m_customSparkSettings.getStringValue();
    }
    

	/**
	 * Parses the custom Spark settings string (see
	 * {@link #getCustomSparkSettingsString()}) into a map and returns it.
	 * 
	 * @return the parsed custom Spark settings as a map.
	 */
	public Map<String, String> getCustomSparkSettings() {
		return SparkPreferenceValidator.parseSettingsString(getCustomSparkSettingsString());
	}

    /**
     * 
     * @return whether to use custom spark settings or not.
     */
    public boolean useCustomSparkSettings() {
        return m_overrideSparkSettings.getBooleanValue();
    }

	/**
	 * 
	 * @return settings model for whether to warn when the local Spark context
	 *         already exists prior to trying to create it.
	 * @see #hideExistsWarning()
	 */
    protected SettingsModelBoolean getHideExistsWarningModel() {
    	return m_hideExistsWarning;
    }

	/**
	 * 
	 * @return whether to warn when the local Spark context
	 *         already exists prior to trying to create it.
	 */
    public boolean hideExistsWarning() {
    	return m_hideExistsWarning.getBooleanValue();
    }
    
	/**
	 * @return the {@link SparkContextID} derived from the context settings.
	 */
	public SparkContextID getSparkContextID() {
		return m_sparkContextID;
	}

	/**
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
    	m_livyUrl.saveSettingsTo(settings);
        m_contextName.saveSettingsTo(settings);
        m_onDisposeAction.saveSettingsTo(settings);
        m_overrideSparkSettings.saveSettingsTo(settings);
        m_customSparkSettings.saveSettingsTo(settings);
        m_hideExistsWarning.saveSettingsTo(settings);
    }

    /**
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    	m_livyUrl.validateSettings(settings);
    	m_contextName.validateSettings(settings);
        m_onDisposeAction.validateSettings(settings);
        m_overrideSparkSettings.validateSettings(settings);
        if (m_overrideSparkSettings.getBooleanValue()) {
            m_customSparkSettings.validateSettings(settings);
        }
        
        m_hideExistsWarning.validateSettings(settings);
        
        final LivySparkContextSettings tmpSettings = new LivySparkContextSettings();
        tmpSettings.loadSettingsFrom(settings);
        tmpSettings.validateDeeper();
    }

    /**
     * Validate current settings values.
     * 
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateDeeper() throws InvalidSettingsException {
    	final List<String> errors = new ArrayList<>();
    	
    	SparkPreferenceValidator.validateSparkContextName(getContextName(), errors);
    	SparkPreferenceValidator.validateCustomSparkSettings(useCustomSparkSettings(), getCustomSparkSettingsString(), errors);
    	
        if (!errors.isEmpty()) {
            throw new InvalidSettingsException(SparkPreferenceValidator.mergeErrors(errors));
        }
    }

	/**
	 * @param settings
	 *            the NodeSettingsRO to read from.
	 * @throws InvalidSettingsException
	 *             if the settings are invalid.
	 */
	public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_livyUrl.loadSettingsFrom(settings);
		m_contextName.loadSettingsFrom(settings);
		m_onDisposeAction.loadSettingsFrom(settings);
		m_overrideSparkSettings.loadSettingsFrom(settings);
		m_customSparkSettings.loadSettingsFrom(settings);
		m_hideExistsWarning.loadSettingsFrom(settings);
		m_sparkContextID = LivySparkContextConfig.createSparkContextID(m_livyUrl.getStringValue(),
				m_contextName.getStringValue());
	}

	/**
	 * @return a new {@link LocalSparkContextConfig} derived from the current
	 *         settings.
	 */
	public LivySparkContextConfig createContextConfig() {
		return new LivySparkContextConfig(
				getLivyUrl(),
				getContextName(),
				getOnDisposeAction() == OnDisposeAction.DELETE_DATA,
				useCustomSparkSettings(),
				getCustomSparkSettings());
	}
}
