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
 */
package org.knime.bigdata.hdfs.filehandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.knime.bigdata.commons.config.CommonConfigContainer;

/**
 * Wraps hadoop default key store and injects client configurations directly (instead of external configuration file).
 *
 * @author Sascha Wolke, KNIME.com
 */
@Deprecated
public class KeyStoreFactoryWrapper extends FileBasedKeyStoresFactory {

    @Override
    public void setConf(final Configuration conf) {
        CommonConfigContainer configContainer = CommonConfigContainer.getInstance();
        if (configContainer.hasSSLConfig()) {
            configContainer.addSSLClientConfig(conf);
        }

        super.setConf(conf);
    }
}
