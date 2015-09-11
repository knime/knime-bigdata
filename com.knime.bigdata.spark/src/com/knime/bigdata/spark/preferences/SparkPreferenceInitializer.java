/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 */
package com.knime.bigdata.spark.preferences;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.jface.preference.IPreferenceStore;

import com.knime.bigdata.spark.SparkPlugin;

/**
 *
 * @author Tobias Koetter, University of Konstanz
 */
public class SparkPreferenceInitializer extends
        AbstractPreferenceInitializer {


    /** Preference key for the database directory setting. */
    public static final String PREF_JOB_SERVER = "com.knime.bigdata.spark.jobServer";

    /** Preference key for the database directory setting. */
    public static final String PREF_JOB_SERVER_PORT = "com.knime.bigdata.spark.jobServer.port";

    /** Preference key for the jdbc url. */
    public static final String PREF_JOB_SERVER_PROTOCOL = "com.knime.bigdata.spark.jobServer.protocol";

    /** Preference key for the jdbc user. */
    public static final String PREF_USER_NAME = "com.knime.bigdata.spark.user";

    /** Preference key for the jdbc user. */
    public static final String PREF_PWD = "com.knime.bigdata.spark.pwd";

    /** Preference key for the usage of network setting. */
    public static final String PREF_CONTEXT_NAME = "com.knime.bigdata.spark.context";

    /** Preference key for the jdbc cache size. */
    public static final String PREF_NUM_CPU_CORES = "com.knime.bigdata.spark.numCpuCores";

    /** Preference key for the jdbc cache size. */
    public static final String PREF_MEM_PER_NODE = "com.knime.bigdata.spark.memperNode";


    /** Preference key for the jdbc cache size. */
    public static final String PREF_JOB_TIMEOUT = "com.knime.bigdata.spark.jobTimeout";

    /** Preference key for the jdbc cache size. */
    public static final String PREF_JOB_CHECK_FREQUENCY = "com.knime.bigdata.spark.jobCheckFrequency";

    /** Preference key for the cache DataCell flag. */
    public static final String PREF_DELETE_RDDS_ON_DISPOSE = "com.knime.bigdata.spark.deleteRDDsOnDispose";


    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeDefaultPreferences() {
        final IPreferenceStore store = SparkPlugin.getDefault().getPreferenceStore();
        //set default values
        store.setDefault(PREF_JOB_SERVER, KNIMEConfigContainer.m_config.getString("spark.jobServer"));
        store.setDefault(PREF_JOB_SERVER_PROTOCOL, KNIMEConfigContainer.m_config.getString("spark.jobServerProtocol"));
        store.setDefault(PREF_JOB_SERVER_PORT, KNIMEConfigContainer.m_config.getInt("spark.jobServerPort"));
        store.setDefault(PREF_USER_NAME, KNIMEConfigContainer.m_config.getString("spark.userName"));
        final String password = KNIMEConfigContainer.m_config.hasPath("spark.password") ?
            KNIMEConfigContainer.m_config.getString("spark.password") : "";
        store.setDefault(PREF_PWD, password);

        final String contextName = KNIMEConfigContainer.m_config.hasPath("spark.contextName") ?
            KNIMEConfigContainer.m_config.getString("spark.contextName") : "knime";
        store.setDefault(PREF_CONTEXT_NAME, contextName);
        store.setDefault(PREF_NUM_CPU_CORES, KNIMEConfigContainer.m_config.getInt("spark.numCPUCores"));
        store.setDefault(PREF_MEM_PER_NODE, KNIMEConfigContainer.m_config.getString("spark.memPerNode"));

        store.setDefault(PREF_JOB_TIMEOUT, KNIMEConfigContainer.m_config.getInt("knime.jobTimeout"));
        store.setDefault(PREF_JOB_CHECK_FREQUENCY, KNIMEConfigContainer.m_config.getInt("knime.jobCheckFrequency"));
        store.setDefault(PREF_DELETE_RDDS_ON_DISPOSE, KNIMEConfigContainer.m_config.getBoolean("knime.deleteRDDsOnDispose"));
    }
}
