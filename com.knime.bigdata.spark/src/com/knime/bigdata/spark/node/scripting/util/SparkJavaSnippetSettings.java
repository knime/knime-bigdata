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
package com.knime.bigdata.spark.node.scripting.util;

import org.knime.base.node.jsnippet.util.JavaFieldList.InColList;
import org.knime.base.node.jsnippet.util.JavaFieldList.InVarList;
import org.knime.base.node.jsnippet.util.JavaFieldList.OutColList;
import org.knime.base.node.jsnippet.util.JavaFieldList.OutVarList;
import org.knime.base.node.jsnippet.util.JavaSnippetFields;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 * @author koetter
 */
public class SparkJavaSnippetSettings {
    private static final String SCRIPT_IMPORTS = "scriptImports";
    private static final String SCRIPT_FIELDS = "scriptFields";
    private static final String SCRIPT_BODY = "scriptBody";
    private static final String JAR_FILES = "jarFiles";
    private static final String OUT_COLS = "outCols";
    private static final String OUT_VARS = "outVars";
    private static final String IN_COLS = "inCols";
    private static final String IN_VARS = "inVars";
    private static final String TEMPLATE_UUID = "templateUUID";
    private static final String VERSION = "version";
    private static final String RUN_ON_EXECUTE = "runOnExecute";

    /** Custom imports. */
    private String m_scriptImports;
    /** Custom fields. */
    private String m_scriptFields;
    /** Custom script. */
    private String m_scriptBody;

    /** Custom jar files. */
    private String[] m_jarFiles;

//    /** Input columns definition. */
    private InColList m_inCols;
//    /** Input variables definitions. */
    private InVarList m_inVars;
//    /** Output columns definition. */
    private OutColList m_outCols;
//    /** Output variables definition. */
    private OutVarList m_outVars;

    /** The UUID of the blueprint for this setting. */
    private String m_templateUUID;

    /** The version of the java snippet. */
    private String m_version;

    /** If Java Edit Variable should be run during execute, not configure. */
    private boolean m_runOnExecute;

    /**
     *
     */
    public SparkJavaSnippetSettings() {
        this("");
    }

    /**
     * Create a new instance.
     * @param defaultContent the default content of the method
     */
    public SparkJavaSnippetSettings(final String defaultContent) {
        m_scriptImports = "// Your custom imports:\n";
        m_scriptFields = "// Your custom variables:\n";
        m_scriptBody = "// Enter your code here:\n\n" + defaultContent + "\n\n";
        m_jarFiles = new String[0];
        m_outCols = new OutColList();
        m_outVars = new OutVarList();
        m_inCols = new InColList();
        m_inVars = new InVarList();
//        m_version = JavaSnippet.VERSION_1_X;
        m_templateUUID = null;
        m_runOnExecute = false;
    }


    /**
     * @return the scriptImports
     */
    String getScriptImports() {
        return m_scriptImports;
    }


    /**
     * @param scriptImports the scriptImports to set
     */
    void setScriptImports(final String scriptImports) {
        m_scriptImports = scriptImports;
    }


    /**
     * @return the scriptFields
     */
    String getScriptFields() {
        return m_scriptFields;
    }


    /**
     * @param scriptFields the scriptFields to set
     */
    void setScriptFields(final String scriptFields) {
        m_scriptFields = scriptFields;
    }


    /**
     * @return the scriptBody
     */
    String getScriptBody() {
        return m_scriptBody;
    }


    /**
     * @param scriptBody the scriptBody to set
     */
    void setScriptBody(final String scriptBody) {
        m_scriptBody = scriptBody;
    }


    /**
     * @return the jarFiles
     */
    public String[] getJarFiles() {
        return m_jarFiles;
    }


    /**
     * @param jarFiles the jarFiles to set
     */
    void setJarFiles(final String[] jarFiles) {
        m_jarFiles = jarFiles;
    }


    /**
     * @return the version
     */
    String getVersion() {
        return m_version;
    }


    /**
     * @param version the version to set
     */
    void setVersion(final String version) {
        m_version = version;
    }

    /**
     * Get the system fields definitions of the java snippet.
     * @return the system fields definitions of the java snippet
     */
    public JavaSnippetFields getJavaSnippetFields() {
        return new JavaSnippetFields(m_inCols, m_inVars, m_outCols, m_outVars);
    }

    /**
     * @return the templateUUID
     */
    public String getTemplateUUID() {
        return m_templateUUID;
    }


    /**
     * @param templateUUID the templateUUID to set
     */
    public void setTemplateUUID(final String templateUUID) {
        m_templateUUID = templateUUID;
    }

    /**
     * @return the m_runOnExecute
     */
    public boolean isRunOnExecute() {
        return m_runOnExecute;
    }

    /**
     * @param runOnExecute the runOnExecute to set
     */
    public void setRunOnExecute(final boolean runOnExecute) {
        m_runOnExecute = runOnExecute;
    }

    /**
     * Set the system fields definitions of the java snippet.
     * @param fields the system fields definitions of the java snippet
     */
    public void setJavaSnippetFields(final JavaSnippetFields fields) {
        m_inCols = fields.getInColFields();
        m_inVars = fields.getInVarFields();
        m_outCols = fields.getOutColFields();
        m_outVars = fields.getOutVarFields();
    }


    /** Saves current parameters to settings object.
     * @param settings To save to.
     */
    public void saveSettings(final NodeSettingsWO settings) {
        settings.addString(SCRIPT_IMPORTS, m_scriptImports);
        settings.addString(SCRIPT_FIELDS, m_scriptFields);
        settings.addString(SCRIPT_BODY, m_scriptBody);
        settings.addStringArray(JAR_FILES, m_jarFiles);
        m_outCols.saveSettings(settings.addConfig(OUT_COLS));
        m_outVars.saveSettings(settings.addConfig(OUT_VARS));
        m_inCols.saveSettings(settings.addConfig(IN_COLS));
        m_inVars.saveSettings(settings.addConfig(IN_VARS));
        settings.addString(VERSION, m_version);
        settings.addString(TEMPLATE_UUID, m_templateUUID);
        settings.addBoolean(RUN_ON_EXECUTE, m_runOnExecute);
    }

    /** Loads parameters in NodeModel.
     * @param settings To load from.
     * @throws InvalidSettingsException If incomplete or wrong.
     */
    public void loadSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_scriptImports = settings.getString(SCRIPT_IMPORTS);
        m_scriptFields = settings.getString(SCRIPT_FIELDS);
        m_scriptBody = settings.getString(SCRIPT_BODY);
        m_jarFiles = settings.getStringArray(JAR_FILES);
        m_outCols.loadSettings(settings.getConfig(OUT_COLS));
        m_outVars.loadSettings(settings.getConfig(OUT_VARS));
        m_inCols.loadSettings(settings.getConfig(IN_COLS));
        m_inVars.loadSettings(settings.getConfig(IN_VARS));
        m_version = settings.getString(VERSION);
        if (settings.containsKey(TEMPLATE_UUID)) {
            m_templateUUID = settings.getString(TEMPLATE_UUID);
        }
        // added in 2.8 (only java edit variable) -- 2.7 scripts were always run on execute()
        m_runOnExecute = settings.getBoolean(RUN_ON_EXECUTE, true);
    }


    /** Loads parameters in Dialog.
     * @param settings To load from.
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        try {
            m_scriptImports = settings.getString(SCRIPT_IMPORTS, "");
            m_scriptFields = settings.getString(SCRIPT_FIELDS, "");
            m_scriptBody = settings.getString(SCRIPT_BODY, null);
            m_jarFiles = settings.getStringArray(JAR_FILES, new String[0]);
            m_outCols.loadSettingsForDialog(settings.getConfig(OUT_COLS));
            m_outVars.loadSettingsForDialog(settings.getConfig(OUT_VARS));
            m_inCols.loadSettingsForDialog(settings.getConfig(IN_COLS));
            m_inVars.loadSettingsForDialog(settings.getConfig(IN_VARS));
//            m_version = settings.getString(VERSION, JavaSnippet.VERSION_1_X);
            m_templateUUID = settings.getString(TEMPLATE_UUID, null);
            // added in 2.8 (only java edit variable)
            m_runOnExecute = settings.getBoolean(RUN_ON_EXECUTE, false);
        } catch (InvalidSettingsException e) {
            throw new IllegalStateException(e);
        }
    }
}
