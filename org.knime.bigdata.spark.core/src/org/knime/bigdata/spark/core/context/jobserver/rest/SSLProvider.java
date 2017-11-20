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
 *   Created on 18.08.2015 by dwk
 */
package org.knime.bigdata.spark.core.context.jobserver.rest;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;

/**
 *
 * @author dwk
 */
public class SSLProvider {

    static SSLContext setupSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
        SecureRestClientTrustManager secureRestClientTrustManager = new SecureRestClientTrustManager();
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, new javax.net.ssl.TrustManager[]{secureRestClientTrustManager}, null);
        return sslContext;
    }

    //not needed
//    static SSLContext setupSSLContextWithKeyStore() {
//        // Create/initialize the SSLContext with key material
//
//        char[] passphrase = "knimeKeyCaps".toCharArray();
//
//        // First initialize the key and trust material.
//        try {
//            KeyStore ksKeys = KeyStore.getInstance("JKS");
//            ksKeys.load(new FileInputStream("clientkeystore"), passphrase);
//            KeyStore ksTrust = KeyStore.getInstance("JKS");
//            ksTrust.load(new FileInputStream("knimeCacerts"), passphrase);
//
//            // KeyManager's decide which key material to use.
//            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
//            kmf.init(ksKeys, passphrase);
//
//            // TrustManager's decide whether to allow connections.
//            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
//            tmf.init(ksTrust);
//
//            SSLContext sslContext = SSLContext.getInstance("TLS");
//            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
//            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
//            return sslContext;
//        } catch (NoSuchAlgorithmException | CertificateException | IOException | KeyStoreException
//                | UnrecoverableKeyException | KeyManagementException e) {
//            //TODO ....
//            return null;
//        }
//    }
}
