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
 *   Created on Aug 13, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.configuration.security.ProxyAuthorizationPolicy;
import org.apache.cxf.interceptor.LoggingInInterceptor;
import org.apache.cxf.interceptor.LoggingOutInterceptor;
import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.common.gzip.GZIPOutInterceptor;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.knime.bigdata.commons.rest.AbstractRESTClient;
import org.knime.core.node.NodeLogger;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * Client to access Databricks REST API using CXF, JAX-RS and Jackson.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksRESTClient extends AbstractRESTClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksRESTClient.class);

    /** Current API version. */
    private static final String API_VERSION = "2.0";

    private DatabricksRESTClient() {}

    /**
     * Map response HTTP status codes to {@link IOException}s with error message from JSON response if possible.
     */
    static class DatabricksResponseExceptionMapper implements ResponseExceptionMapper<IOException> {
        @Override
        public IOException fromResponse(final Response response) {
            String message = "";

            // try to parse JSON response with error message
            if (response.getMediaType().getSubtype().toLowerCase().contains("json")) {
                try {
                    message = response.readEntity(GenericErrorResponse.class).message;
                } catch (Exception e) {
                    message = e.getMessage();
                }
            }

            if (response.getStatus() == 403 && !StringUtils.isBlank(message)) {
                return new AccessDeniedException(message);
            } else if (response.getStatus() == 403) {
                return new AccessDeniedException("Invalid or missing authentication data");
            } else if (response.getStatus() == 404 && !StringUtils.isBlank(message)) {
                return new FileNotFoundException(message);
            } else if (response.getStatus() == 404) {
                return new FileNotFoundException("Resource not found");
            } else if (!StringUtils.isBlank(message)) {
                return new IOException("Server error: " + message);
            } else {
                return new IOException("Server error: " + response.getStatus());
            }
        }
    }

    /**
     * Creates a service proxy for given Databricks REST API interface without any authentication data. Use
     * {@link DatabricksRESTClient#create(String, Class, String, int, int)} or
     * {@link DatabricksRESTClient#create(String, Class, String, String, int, int)} instead.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     */
    private static <T> T create(final String deploymentUrl, final Class<T> proxy,
            final int receiveTimeoutMillis, final int connectionTimeoutMillis) {

        final String baseUrl = deploymentUrl + "/api/" + API_VERSION;

        final HTTPClientPolicy clientPolicy = createClientPolicy(receiveTimeoutMillis, connectionTimeoutMillis);
        final ProxyAuthorizationPolicy proxyAuthPolicy = configureProxyIfNecessary(baseUrl, clientPolicy);

        // Create the API Proxy
        final List<Object> provider = Arrays.asList(new JacksonJsonProvider(), new DatabricksResponseExceptionMapper());
        final T proxyImpl = JAXRSClientFactory.create(baseUrl, proxy, provider);
        WebClient.client(proxyImpl)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .header("User-Agent", getUserAgent());
        final ClientConfiguration config = WebClient.getConfig(proxyImpl);
        config.getInInterceptors().add(new GZIPInInterceptor());
        config.getOutInterceptors().add(new GZIPOutInterceptor());
        config.getHttpConduit().setClient(clientPolicy);
        if (proxyAuthPolicy != null) {
            config.getHttpConduit().setProxyAuthorization(proxyAuthPolicy);
        }
        if (LOG.isDebugEnabled()) {
            config.getInInterceptors().add(new LoggingInInterceptor());
            config.getOutInterceptors().add(new LoggingOutInterceptor());
        }

        return proxyImpl;
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using a bearer authentication token.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param token Authentication token
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String token,
        final int receiveTimeoutMillis, final int connectionTimeoutMillis) {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeoutMillis, connectionTimeoutMillis);
        WebClient.client(proxyImpl).header("Authorization", "Bearer " + token);
        return proxyImpl;
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using basic authentication with user and
     * password.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param user Username for authentication
     * @param password Password for authentication
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     * @throws UnsupportedEncodingException if given user and password can't be encoded as UTF-8
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String user, final String password,
        final int receiveTimeoutMillis, final int connectionTimeoutMillis) throws UnsupportedEncodingException {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeoutMillis, connectionTimeoutMillis);
        WebClient.client(proxyImpl).header("Authorization", "Basic " + Base64Utility.encode((user + ":" + password).getBytes("UTF-8")));
        return proxyImpl;
    }


    /**
     * Release the internal state and configuration associated with this service proxy.
     *
     * @param proxy Client proxy implementation
     */
    public static <T> void close(final T proxy) {
        WebClient.client(proxy).close();
    }
}
