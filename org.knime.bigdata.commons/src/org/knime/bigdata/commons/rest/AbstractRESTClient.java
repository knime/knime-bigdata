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
 *   Sep 5, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.commons.rest;

import java.net.URI;

import org.apache.cxf.configuration.security.ProxyAuthorizationPolicy;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.transports.http.configuration.ProxyServerType;
import org.eclipse.core.net.proxy.IProxyData;
import org.eclipse.core.net.proxy.IProxyService;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.Version;
import org.osgi.util.tracker.ServiceTracker;

/**
 * Abstract CXF/JAXRS REST client.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AbstractRESTClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(AbstractRESTClient.class);

    /** Chunk threshold in bytes. */
    private static final int CHUNK_THRESHOLD = 10 * 1024 * 1024; // 10MB

    /** Length in bytes of each chunk. */
    private static final int CHUNK_LENGTH = 1 * 1024 * 1024; // 1MB

    private static final Version CLIENT_VERSION = FrameworkUtil.getBundle(AbstractRESTClient.class).getVersion();
    private static final String USER_AGENT = "KNIME/" + CLIENT_VERSION;

    private static final ServiceTracker<IProxyService, IProxyService> PROXY_TRACKER;
    static {
        PROXY_TRACKER = new ServiceTracker<>(FrameworkUtil.getBundle(AbstractRESTClient.class).getBundleContext(),
            IProxyService.class, null);
        PROXY_TRACKER.open();
    }

    /**
     * @return HTTP User-Agent name
     */
    protected static String getUserAgent() {
        return USER_AGENT;
    }

    /**
     * @param receiveTimeoutMillis receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return default {@link HTTPClientPolicy} to use REST clients
     */
    protected static HTTPClientPolicy createClientPolicy(final long receiveTimeoutMillis, final long connectionTimeoutMillis) {
        final HTTPClientPolicy clientPolicy = new HTTPClientPolicy();
        clientPolicy.setAllowChunking(true);
        clientPolicy.setChunkingThreshold(CHUNK_THRESHOLD);
        clientPolicy.setChunkLength(CHUNK_LENGTH);
        clientPolicy.setReceiveTimeout(receiveTimeoutMillis);
        clientPolicy.setConnectionTimeout(connectionTimeoutMillis);
        return clientPolicy;
    }

    /**
     * Configures HTTP proxy on given client policy and returns proxy authorization policy if required or
     * <code>null</code>.
     *
     * @param url Base URL to configure proxy for
     * @param clientPolicy Policy to apply proxy configuration to
     * @return {@link ProxyAuthorizationPolicy} or <code>null</code> if no proxy is used or nor proxy authentication is
     *         required
     */
    protected static ProxyAuthorizationPolicy configureProxyIfNecessary(final String url,
            final HTTPClientPolicy clientPolicy) {

        final URI uri = URI.create(url);
        ProxyAuthorizationPolicy proxyAuthPolicy = null;

        IProxyService proxyService = PROXY_TRACKER.getService();
        if (proxyService == null) {
            LOG.error("No Proxy service registered in Eclipse framework. Not using any proxies for databricks connection.");
            return null;
        }

        for (IProxyData proxy : proxyService.select(uri)) {

            final ProxyServerType proxyType;
            final int defaultPort;

            switch (proxy.getType()) {
                case IProxyData.HTTP_PROXY_TYPE:
                    proxyType = ProxyServerType.HTTP;
                    defaultPort = 80;
                    break;
                case IProxyData.HTTPS_PROXY_TYPE:
                    proxyType = ProxyServerType.HTTP;
                    defaultPort = 443;
                    break;
                case IProxyData.SOCKS_PROXY_TYPE:
                    proxyType = ProxyServerType.SOCKS;
                    defaultPort = 1080;
                    break;
                default:
                    throw new UnsupportedOperationException(String.format(
                        "Unsupported proxy type: %s. Please remove the proxy setting from File > Preferences > General > Network Connections.",
                        proxy.getType()));
            }

            if (proxy.getHost() != null) {
                clientPolicy.setProxyServerType(proxyType);
                clientPolicy.setProxyServer(proxy.getHost());

                clientPolicy.setProxyServerPort(defaultPort);
                if (proxy.getPort() != -1) {
                    clientPolicy.setProxyServerPort(proxy.getPort());
                }

                if (proxy.isRequiresAuthentication() && proxy.getUserId() != null && proxy.getPassword() != null) {
                    proxyAuthPolicy = new ProxyAuthorizationPolicy();
                    proxyAuthPolicy.setUserName(proxy.getUserId());
                    proxyAuthPolicy.setPassword(proxy.getPassword());
                }

                LOG.debug(String.format(
                    "Using proxy for REST connection to %s: %s:%d (type: %s, proxyAuthentication: %b)",
                    url, clientPolicy.getProxyServer(), clientPolicy.getProxyServerPort(), proxy.getType(),
                    proxyAuthPolicy != null));

                break;
            }
        }

        return proxyAuthPolicy;
    }
}
