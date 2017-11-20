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
 *   Created on 18.08.2015 by Dietrich
 */
package com.knime.bigdata.spark.core.context.jobserver.rest;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * Accepts all servers independent of their certificates.
 * This is ok since the server is defined by the user in the preferences.
 *
 */
public class SecureRestClientTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(final X509Certificate[] arg0, final String arg1)
            throws CertificateException {
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] arg0, final String arg1)
            throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
