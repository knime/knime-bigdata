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
package org.knime.bigdata.spark.core.context.jobserver.rest;

import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.message.Message;
import org.apache.cxf.transport.http.auth.HttpAuthSupplier;

/**
 * HTTP basic authentication supplier.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class WsRsRestClientAuthSupplier implements HttpAuthSupplier {

    private final String m_authenticationToken;

    /**
     * Default constructor.
     * @param user - Username to use
     * @param password - Password to use
     * @throws UnsupportedEncodingException - If UTF-8 encoding with username and password fails.
     */
    public WsRsRestClientAuthSupplier(final String user, final String password) throws UnsupportedEncodingException {
        m_authenticationToken = "Basic " + Base64Utility.encode((user + ":" + password).getBytes("UTF-8"));
    }

    @Override
    public boolean requiresRequestCaching() {
        return false;
    }

    @Override
    public String getAuthorization(final AuthorizationPolicy authPolicy, final URI url, final Message message, final String fullHeader) {
        return m_authenticationToken;
    }
}
