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
 */
package org.knime.bigdata.filehandling.knox.rest;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.interceptor.LoggingInInterceptor;
import org.apache.cxf.interceptor.LoggingOutInterceptor;
import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.knime.bigdata.commons.rest.AbstractRESTClient;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

/**
 * Web HDFS via KNOX REST client.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHDFSClient extends AbstractRESTClient {

    private static final NodeLogger LOG = NodeLogger.getLogger(KnoxHDFSClient.class);

    /**
     * Map response HTTP status codes to {@link IOException}s with error message from JSON response if possible.
     */
    static class KNOXResponseExceptionMapper implements ResponseExceptionMapper<ClientErrorException> {
        @Override
        public ClientErrorException fromResponse(final Response response) {
            final ClientErrorException toReturn;
            switch (Status.fromStatusCode(response.getStatus())) {
                case UNAUTHORIZED:
                    toReturn = new NotAuthorizedException(response);
                    break;
                case FORBIDDEN:
                    toReturn = new ForbiddenException(response);
                    break;
                case NOT_FOUND:
                    toReturn = new NotFoundException(response);
                    break;
                default:
                    toReturn = new ClientErrorException(response);
            }

            return toReturn;
        }
    }

    /**
     * Creates a service proxy for WebHDFS REST API interface without any authentication data.
     *
     * @param baseUrl https://...:8433/gateway/.../webhdfs/v1/
     * @param receiveTimeoutMillis Receive timeout
     * @param connectionTimeoutMillis connection timeout
     * @see KnoxHDFSClient#createClientBasicAuth(String, String, String, int, int)
     * @return WebHDFS REST API proxy
     */
    private static WebHDFSAPI createClient(final String baseUrl,
        final Duration receiveTimeoutMillis, final Duration connectionTimeoutMillis) {

        final HTTPClientPolicy clientPolicy = createClientPolicy(receiveTimeoutMillis, connectionTimeoutMillis);

        // Create the API Proxy
        final JacksonJsonProvider jsonProvider = new JacksonJsonProvider();
        jsonProvider.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);
        final List<Object> provider = Arrays.asList(jsonProvider, new KNOXResponseExceptionMapper());
        final WebHDFSAPI proxyImpl = JAXRSClientFactory.create(baseUrl, WebHDFSAPI.class, provider);
        WebClient.client(proxyImpl)
            .accept(MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .header("User-Agent", getUserAgent());
        final ClientConfiguration config = WebClient.getConfig(proxyImpl);
        // Enable cookie handling
        config.getRequestContext().put(org.apache.cxf.message.Message.MAINTAIN_SESSION, Boolean.TRUE);
        config.getInInterceptors().add(new GZIPInInterceptor());
        config.getHttpConduit().setClient(clientPolicy);
        if (LOG.isDebugEnabled()) {
            config.getInInterceptors().add(new LoggingInInterceptor());
            config.getOutInterceptors().add(new LoggingOutInterceptor());
        }

        return proxyImpl;
    }

    /**
     * Creates a service proxy for WebHDFS REST API interface using basic authentication with user and password.
     *
     * @param baseUrl https://...:8433/gateway/.../webhdfs/v1/
     * @param user Username for authentication
     * @param password Password for authentication
     * @param receiveTimeoutMillis Receive timeout
     * @param connectionTimeoutMillis connection timeout
     * @return WebHDFS REST API proxy
     */
    public static WebHDFSAPI createClientBasicAuth(final String baseUrl, final String user, final String password,
        final Duration receiveTimeoutMillis, final Duration connectionTimeoutMillis) {

        final WebHDFSAPI proxyImpl = createClient(baseUrl, receiveTimeoutMillis, connectionTimeoutMillis);
        if (!StringUtils.isBlank(user) && !StringUtils.isBlank(password)) {
            WebClient.client(proxyImpl).header("Authorization", "Basic " + Base64Utility.encode((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        } else if (!StringUtils.isBlank(user)) {
            WebClient.client(proxyImpl).header("Authorization", "Basic " + Base64Utility.encode((user + ":").getBytes(StandardCharsets.UTF_8)));
        }
        return new WebHDFSAPIWrapper(proxyImpl);
    }

    /**
     * Read a file.
     */
    public static InputStream openFile(final WebHDFSAPI proxyImpl, final String path) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response respOpen = proxyImpl.open(path, GetOpParam.Op.OPEN, CHUNK_LENGTH)) {
            validateStatusCode(respOpen, 307);
            final WebHDFSAPI proxyToUse = (proxyImpl instanceof WebHDFSAPIWrapper) //
                ? ((WebHDFSAPIWrapper)proxyImpl).getWrappedWebHDFsAPI() //
                : proxyImpl;

            final Response respRead = WebClient.fromClient(WebClient.client(proxyToUse), true) //
                .to(respOpen.getLocation().toString(), false) //
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE) //
                .get();
            validateStatusCode(respRead, 200);

            final Object entity = respRead.getEntity();
            if (entity instanceof InputStream) {
                return new BufferedInputStream((InputStream)entity, CHUNK_LENGTH);
            } else {
                throw new IOException("Unknown entity received: " + entity.getClass().getName());
            }
        }
    }

    /**
     * Create files asynchrony.
     *
     * The JAX-RS/CXF client supports async execution only at response receive time and not in the request/send time.
     * The implementation use an {@link ExecutorService} to run the upload in the background to avoid the blocking
     * upload call and returns the output stream immediately. The status code of the response will be checked after the
     * output stream was closed.
     *
     * @param proxyImpl client to use
     * @param executor executor service to run the asynchrony upload
     * @param path destination path
     * @param overwrite {@code true} if an existing file should be overwritten
     * @return output stream to append data to
     * @throws IOException
     */
    public static OutputStream createFile(final WebHDFSAPI proxyImpl, final ExecutorService executor, final String path,
        final boolean overwrite) throws IOException {

        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response respCreate = proxyImpl.create(path, PutOpParam.Op.CREATE, CHUNK_LENGTH, overwrite)) {
            validateStatusCode(respCreate, 307);
            return uploadFile(proxyImpl, HttpMethod.PUT, respCreate.getLocation(), 201, executor);
        }
    }

    /**
     * Append asynchrony to files.
     *
     * The JAX-RS/CXF client supports async execution only at response receive time and not in the request/send time.
     * The implementation use an {@link ExecutorService} to run the upload in the background to avoid the blocking
     * upload call and returns the output stream immediately. The status code of the response will be checked after the
     * output stream was closed.
     *
     * @param proxyImpl client to use
     * @param executor executor service to run the asynchrony upload
     * @param path destination path
     * @return output stream to append data to
     * @throws IOException
     */
    public static OutputStream appendFile(final WebHDFSAPI proxyImpl, final ExecutorService executor, final String path)
        throws IOException {

        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response respCreate = proxyImpl.append(path, PostOpParam.Op.APPEND, CHUNK_LENGTH)) {
            validateStatusCode(respCreate, 307);
            return uploadFile(proxyImpl, HttpMethod.POST, respCreate.getLocation(), 200, executor);
        }
    }

    @SuppressWarnings("resource")
    static OutputStream uploadFile(final WebHDFSAPI proxyImpl, final String method, final URI location,
        final int expectedResponseCode, final ExecutorService executor) throws IOException {

        final UploadOutputStream outputStream = new UploadOutputStream(expectedResponseCode);
        final PipedInputStream inputStream = new PipedInputStream(outputStream, CHUNK_LENGTH);
        final Future<Response> respWrite = executor.submit(() -> {
            final WebHDFSAPI proxyToUse = (proxyImpl instanceof WebHDFSAPIWrapper) //
                    ? ((WebHDFSAPIWrapper)proxyImpl).getWrappedWebHDFsAPI() //
                    : proxyImpl;

            try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
                return WebClient.fromClient(WebClient.client(proxyToUse), true)
                    .to(location.toString(), false)
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .invoke(method, Entity.entity(inputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
        });
        outputStream.setUploadResponse(respWrite);

        return outputStream;
    }

    /**
     * Validate status code of a given response.
     *
     * @throws IOException if response contains the wrong status code
     */
    private static void validateStatusCode(final Response resp, final int expectedCode) throws IOException {
        if (resp.getStatus() == 403) {
            throw new AccessDeniedException("Access denied: " + resp.getStatusInfo().getReasonPhrase());
        } else if (resp.getStatus() == 404) {
            throw new FileNotFoundException("Resource not found: ");
        } else if (resp.getStatus() != expectedCode) {
            throw new IOException(
                String.format("Unknown response from server: expected=%d, got=%d (%s)",
                    expectedCode,
                    resp.getStatus(),
                    resp.getStatusInfo().getReasonPhrase()));
        }
    }

    /**
     * Release the internal state and configuration associated with this service proxy.
     *
     * @param proxy Client proxy implementation
     */
    public static void close(final WebHDFSAPI proxy) {
        WebHDFSAPI toClose = proxy;
        if (proxy instanceof WebHDFSAPIWrapper) {
            toClose = ((WebHDFSAPIWrapper)proxy).getWrappedApiClient();
        }
        WebClient.client(toClose).close();
    }
}
