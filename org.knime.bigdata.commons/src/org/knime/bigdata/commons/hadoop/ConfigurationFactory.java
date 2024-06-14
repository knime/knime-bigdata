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
 *
 * History
 *   May 4, 2018 (bjoern): created
 */
package org.knime.bigdata.commons.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.core.node.NodeLogger;

/**
 * Provides factory methods to create Hadoop {@link Configuration} objects. All methods here ensure that KNIME-wide
 * preferences are applied.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @since 3.6
 */
public class ConfigurationFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ConfigurationFactory.class);

    /**
     *
     * @return the base configuration (see {@link #createBaseConfiguration()}) but with KERBEROS authentication set.
     */
    public static Configuration createBaseConfigurationWithKerberosAuth() {

        final Configuration conf = createBaseConfiguration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.KERBEROS.name());

        // BD-1268: default setting from Hadoop 2.x, allow all namenode kerberos principals
        conf.setStrings("dfs.namenode.kerberos.principal.pattern", "*");

        return conf;
    }

    /**
     *
     * @return the base configuration (see {@link #createBaseConfiguration()}) but with SIMPLE authentication set.
     */
    public static Configuration createBaseConfigurationWithSimpleAuth() {

        final Configuration conf = createBaseConfiguration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.SIMPLE.name());

        return conf;
    }

    /**
     *
     * @return the default Hadoop configuration with the core-site.xml and hdfs-site.xml that can be optionally set in
     *         the KNIME preferences added on top.
     */
    public static Configuration createBaseConfiguration() {
        final Configuration conf = new Configuration();

        final CommonConfigContainer configContainer = CommonConfigContainer.getInstance();
        if (configContainer.hasCoreSiteConfig()) {
            LOGGER.debug("Applying core-site.xml from KNIME preferences");
            conf.addResource(configContainer.getCoreSiteConfig());
        }
        if (configContainer.hasHdfsSiteConfig()) {
            LOGGER.debug("Applying hdfs-site.xml from KNIME preferences");
            conf.addResource(configContainer.getHdfsSiteConfig());
        }
        return conf;
    }
}
