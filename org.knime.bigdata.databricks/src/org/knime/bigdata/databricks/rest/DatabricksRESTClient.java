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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.helpers.CastUtils;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.http.asyncclient.AsyncHTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.knime.bigdata.commons.rest.AbstractRESTClient;
import org.knime.bigdata.databricks.DatabricksPlugin;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.rest.catalog.CatalogAPI;
import org.knime.bigdata.databricks.rest.catalog.CatalogAPIWrapper;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPIWrapper;
import org.knime.bigdata.databricks.rest.commands.CommandsAPI;
import org.knime.bigdata.databricks.rest.commands.CommandsAPIWrapper;
import org.knime.bigdata.databricks.rest.contexts.ContextsAPI;
import org.knime.bigdata.databricks.rest.contexts.ContextsAPIWrapper;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPIWrapper;
import org.knime.bigdata.databricks.rest.files.FilesAPI;
import org.knime.bigdata.databricks.rest.files.FilesAPIWrapper;
import org.knime.bigdata.databricks.rest.jobs.JobsAPI;
import org.knime.bigdata.databricks.rest.jobs.JobsAPIWrapper;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPI;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPIWrapper;
import org.knime.bigdata.databricks.rest.scim.ScimAPI;
import org.knime.bigdata.databricks.rest.scim.ScimAPIWrapper;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseAPI;
import org.knime.bigdata.databricks.rest.sql.SQLWarehouseAPIWrapper;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import jakarta.ws.rs.core.MediaType;

/**
 * Client to access Databricks REST API using CXF, JAX-RS and Jackson.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksRESTClient extends AbstractRESTClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksRESTClient.class);

    private DatabricksRESTClient() {
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
    private static <T> T create(final String deploymentUrl, final Class<T> proxy, final Duration receiveTimeout,
        final Duration connectionTimeout, final ResponseExceptionMapper<?> exceptionMapper) {

        final String baseUrl = deploymentUrl + "/api/";
        final HTTPClientPolicy clientPolicy = createClientPolicy(receiveTimeout, connectionTimeout);

        // Create the API Proxy
        final List<Object> provider = Arrays.asList(new JacksonJsonProvider(), exceptionMapper);
        final T proxyImpl = JAXRSClientFactory.create(baseUrl, proxy, provider);
        WebClient.client(proxyImpl).accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE)
            .header("User-Agent", DatabricksPlugin.getUserAgent());
        final ClientConfiguration config = WebClient.getConfig(proxyImpl);
        config.getInInterceptors().add(new GZIPInInterceptor());
        // Note: Databricks use GZIP to encode downloads, but does not support GZIP encoded uploads!
        config.getHttpConduit().setClient(clientPolicy);

        // Enable request logging:
        // config.getInInterceptors().add(new LoggingInInterceptor());
        // config.getOutInterceptors().add(new LoggingOutInterceptor());

        // This forces usage of the Apache HTTP client over the JDK built-in HTTP client,
        // that does not work well with the strange configured Databricks HTTP/2 endpoint, see BD-1242.
        config.getRequestContext().put(AsyncHTTPConduit.USE_ASYNC, Boolean.TRUE);

        return proxyImpl;
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using a bearer authentication token.
     *
     * Note that errors in this client are handled with {@code IOException}.
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

        final T proxyImpl =
            create(deploymentUrl, proxy, receiveTimeout, connectionTimeout, new DatabricksResponseIOExceptionMapper());
        WebClient.client(proxyImpl).header("Authorization", "Bearer " + token);
        return wrap(proxyImpl);
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using basic authentication with user and
     * password.
     *
     * Note that errors in this client are handled with {@code IOException}.
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
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String user,
        final String password, final Duration receiveTimeout, final Duration connectionTimeout)
        throws UnsupportedEncodingException {

        final T proxyImpl =
            create(deploymentUrl, proxy, receiveTimeout, connectionTimeout, new DatabricksResponseIOExceptionMapper());
        WebClient.client(proxyImpl).header("Authorization",
            "Basic " + Base64Utility.encode((user + ":" + password).getBytes("UTF-8")));
        return wrap(proxyImpl);
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using the URL and credentials of a @link
     * DatabricksWorkspaceAccessor}.
     *
     * Note that errors in this client are handled with the legacy {@code IOException} mapper.
     *
     * @param accessor A {@link DatabricksAccessTokenCredential} which provides both the Databricks workspace URL as
     *            well as credentials.
     * @param proxy Interface to create proxy for
     * @param receiveTimeout Receive timeout
     * @param connectionTimeout connection timeout
     * @return client implementation for given proxy interface
     */
    public static <T> T createWithLegacyExceptionMapper(final DatabricksAccessTokenCredential accessor,
        final Class<T> proxy, final Duration receiveTimeout, final Duration connectionTimeout) {

        return create(accessor, proxy, receiveTimeout, connectionTimeout, new DatabricksResponseIOExceptionMapper());
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using the URL and credentials of a @link
     * DatabricksWorkspaceAccessor}.
     *
     * Note that errors in this client are handled with {@code ClientErrorException}.
     *
     * @param accessor A {@link DatabricksAccessTokenCredential} which provides both the Databricks workspace URL as
     *            well as credentials.
     * @param proxy Interface to create proxy for
     * @param receiveTimeout Receive timeout
     * @param connectionTimeout connection timeout
     * @return client implementation for given proxy interface
     */
    public static <T> T create(final DatabricksAccessTokenCredential accessor, final Class<T> proxy,
        final Duration receiveTimeout, final Duration connectionTimeout) {

        return create(accessor, proxy, receiveTimeout, connectionTimeout,
            new DatabricksResponseClientErrorExceptionMapper());
    }

    private static <T> T create(final DatabricksAccessTokenCredential accessor, final Class<T> proxy,
        final Duration receiveTimeout, final Duration connectionTimeout,
        final ResponseExceptionMapper<?> exceptionMapper) {

        final T proxyImpl = create(accessor.getDatabricksWorkspaceUrl().toString(), //
            proxy, //
            receiveTimeout, //
            connectionTimeout, //
            exceptionMapper);

        WebClient.getConfig(proxyImpl)//
            .getOutInterceptors()//
            .add(new DatabricksCredentialInterceptor(accessor));

        return wrap(proxyImpl);
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using a single Workspace connection input port.
     *
     * Note that errors in this client are handled with {@code ClientErrorException}.
     *
     * @param proxy Interface to create proxy for
     * @param inSpecs the input specs with a single input port
     * @return client implementation for given proxy interface
     * @throws InvalidSettingsException if the input port is invalid
     * @throws NoSuchCredentialException if the input port does not contain a {@link DatabricksAccessTokenCredential}
     */
    public static <T> T fromSingleWorkspaceInputPort(final Class<T> proxy, final PortObjectSpec[] inSpecs)
        throws InvalidSettingsException, NoSuchCredentialException {

        if (inSpecs.length == 0) {
            throw new InvalidSettingsException("Missing input connection, Databricks Workspace Connector required.");
        }

        if (inSpecs[0] instanceof DatabricksWorkspacePortObjectSpec) {
            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
            final DatabricksAccessTokenCredential credential =
                spec.resolveCredential(DatabricksAccessTokenCredential.class);
            return create(credential, proxy, spec.getReadTimeout(), spec.getConnectionTimeout());

        }

        throw new InvalidSettingsException("Invalid input connection, Databricks Workspace Connector required.");
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrap(final T api) { // NOSONAR ignore to many returns
        if (api instanceof CatalogAPI) {
            return (T)new CatalogAPIWrapper((CatalogAPI)api);
        } else if (api instanceof ClusterAPI) {
            return (T)new ClusterAPIWrapper((ClusterAPI)api);
        } else if (api instanceof CommandsAPI) {
            return (T)new CommandsAPIWrapper((CommandsAPI)api);
        } else if (api instanceof ContextsAPI) {
            return (T)new ContextsAPIWrapper((ContextsAPI)api);
        } else if (api instanceof DBFSAPI) {
            return (T)new DBFSAPIWrapper((DBFSAPI)api);
        } else if (api instanceof FilesAPI) {
            return (T)new FilesAPIWrapper((FilesAPI)api);
        } else if (api instanceof JobsAPI) {
            return (T)new JobsAPIWrapper((JobsAPI)api);
        } else if (api instanceof LibrariesAPI) {
            return (T)new LibrariesAPIWrapper((LibrariesAPI)api);
        } else if (api instanceof ScimAPI) {
            return (T)new ScimAPIWrapper((ScimAPI)api);
        } else if (api instanceof SQLWarehouseAPI) {
            return (T)new SQLWarehouseAPIWrapper((SQLWarehouseAPI)api);
        }

        throw new IllegalArgumentException("Unsupported API: " + api.getClass());
    }

    /**
     * Release the internal state and configuration associated with this service proxy.
     *
     * @param proxy Client proxy implementation
     */
    public static <T> void close(final T proxy) {
        Object toClose = proxy;
        if (proxy instanceof APIWrapper) {
            toClose = ((APIWrapper<?>)proxy).getWrappedAPI();
        }
        WebClient.client(toClose).close();
    }

    private static class DatabricksCredentialInterceptor extends AbstractPhaseInterceptor<Message> {

        final DatabricksAccessTokenCredential m_workspaceAccessor;

        DatabricksCredentialInterceptor(final DatabricksAccessTokenCredential workspaceAccessor) {
            super(Phase.SETUP);
            m_workspaceAccessor = workspaceAccessor;
        }

        @Override
        public void handleMessage(final Message message) throws Fault {
            @SuppressWarnings("unchecked")
            final Map<String, List<Object>> headers =
                CastUtils.cast((Map<String, List<Object>>)message.get(Message.PROTOCOL_HEADERS));

            try {
                final String authHeader = String.format("%s %s", //
                    m_workspaceAccessor.getAuthScheme(), m_workspaceAccessor.getAuthParameters());
                headers.put("Authorization", List.of(authHeader));
            } catch (IOException ex) {
                throw new Fault(ex);
            }
        }
    }

}
