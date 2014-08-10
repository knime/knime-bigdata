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
 *   Created on 14.05.2014 by thor
 */
package com.knime.bigdata.hive;

import org.knime.core.node.InvalidSettingsException;

import com.knime.licenses.LicenseFeatures;
import com.knime.licenses.LicenseStore;
import com.knime.licenses.LicenseStoreListener;

/**
 * Simple class that holds the license information.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
public final class LicenseUtil implements LicenseStoreListener {
    /**
     * Singleton instance.
     */
    public static final LicenseUtil instance = new LicenseUtil();

    private String m_licenseError = LicenseStore.getDefaultStore().checkLicense(
        LicenseFeatures.HiveConnector);

    private LicenseUtil() {
        LicenseStore.getDefaultStore().addListener(this);
    }


    /**
     * Checks if a valid Hive license exists and throws an exception with an appropriate error message otherwise.
     *
     * @throws InvalidSettingsException if no valid Hive license exists
     */
    public void checkLicense() throws InvalidSettingsException {
        if (m_licenseError != null) {
            throw new InvalidSettingsException(m_licenseError);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void licensesChanged(final LicenseStore store) {
        m_licenseError = store.checkLicense(LicenseFeatures.HiveConnector);
    }
}
