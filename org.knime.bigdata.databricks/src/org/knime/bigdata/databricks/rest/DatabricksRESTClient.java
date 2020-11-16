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
 *   Created on Aug 13, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
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
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.knime.bigdata.commons.rest.AbstractRESTClient;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPIWrapper;
import org.knime.core.node.NodeLogger;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * Client to access Databricks REST API using CXF, JAX-RS and Jackson.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksRESTClient extends AbstractRESTClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksRESTClient.class);

    private DatabricksRESTClient() {}

    /**
     * Map response HTTP status codes to {@link IOException}s with error message from JSON response if possible.
     */
    static class DatabricksResponseExceptionMapper implements ResponseExceptionMapper<IOException> {
        @Override
        public IOException fromResponse(final Response response) {
            String message = "";

            // try to parse JSON response with error (REST 1.2 API) or message (REST 2.0 API) field
            if (response.getMediaType().getSubtype().toLowerCase().contains("json")) {
                try {
                    final GenericErrorResponse resp = response.readEntity(GenericErrorResponse.class);
                    if (!StringUtils.isBlank(resp.message)) {
                        message = resp.message;
                    } else if (!StringUtils.isBlank(resp.error)) {
                        message = resp.error;
                    } else {
                        message = response.getStatusInfo().getReasonPhrase();
                    }
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
            } else if (response.getStatus() == 500 && message.startsWith("ContextNotFound: ")) {
                return new FileNotFoundException("Context not found");
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
     * @param receiveTimeout Receive timeout
     * @param connectionTimeout connection timeout
     * @return Client implementation for given proxy interface
     */
    private static <T> T create(final String deploymentUrl, final Class<T> proxy,
        final Duration receiveTimeout, final Duration connectionTimeout, final boolean doRequestLogging) {

        final String baseUrl = deploymentUrl + "/api/";

        final HTTPClientPolicy clientPolicy = createClientPolicy(receiveTimeout, connectionTimeout);
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
        // Note: Databricks use GZIP to encode downloads, but does not support GZIP encoded uploads!
        config.getHttpConduit().setClient(clientPolicy);
        if (proxyAuthPolicy != null) {
            config.getHttpConduit().setProxyAuthorization(proxyAuthPolicy);
        }

        if (doRequestLogging) {
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
     * @param receiveTimeout Receive timeout
     * @param connectionTimeout connection timeout
     * @return Client implementation for given proxy interface
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String token,
        final Duration receiveTimeout, final Duration connectionTimeout) {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeout, connectionTimeout, false);
        WebClient.client(proxyImpl).header("Authorization", "Bearer " + token);
        return wrap(proxyImpl);
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using basic authentication with user and
     * password.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param user Username for authentication
     * @param password Password for authentication
     * @param receiveTimeout Receive timeout
     * @param connectionTimeout connection timeout
     * @return Client implementation for given proxy interface
     * @throws UnsupportedEncodingException if given user and password can't be encoded as UTF-8
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String user, final String password,
        final Duration receiveTimeout, final Duration connectionTimeout) throws UnsupportedEncodingException {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeout, connectionTimeout, false);
        WebClient.client(proxyImpl).header("Authorization", "Basic " + Base64Utility.encode((user + ":" + password).getBytes("UTF-8")));
        return wrap(proxyImpl);
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrap(final T api) {
        if (api instanceof DBFSAPI) {
            return (T)new DBFSAPIWrapper((DBFSAPI)api);
        }
        return api;
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
