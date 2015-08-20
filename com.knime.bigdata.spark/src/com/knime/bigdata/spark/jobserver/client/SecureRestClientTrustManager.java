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
 *   Created on 18.08.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.client;

/**
 * Copyright (c) 2008 - 2011 Bhaveshkumar Thaker [http://bhaveshthaker.com].  All rights reserved.
 */

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * @author <a href="http://bhaveshthaker.com/">Bhavesh Thaker</a>
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
