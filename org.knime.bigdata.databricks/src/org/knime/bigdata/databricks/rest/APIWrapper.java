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
 *   2024-05-15 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.rest;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

import org.knime.bigdata.database.databricks.DatabricksRateLimitClientErrorException;
import org.knime.bigdata.database.databricks.DatabricksRateLimitException;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

/**
 * Base API wrapper class that prevents authentication popups.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 *
 * @param <T> type of wrapped API
 */
public class APIWrapper<T> {

    private static final long MAX_WAIT_TIME = 60 * 1000l; // 60 seconds

    private static final NodeLogger m_logger = NodeLogger.getLogger(DatabricksRESTClient.class); // NOSONAR use a more specific logger name instead of APIWrapper

    /**
     * Wrapped API
     */
    protected final T m_api;

    /**
     * Name of the API endpoint, used in logging messages.
     */
    private final String m_apiName;

    /**
     * Default constructor.
     *
     * @param api the API to wrap
     * @param apiName the name of the API endpoint
     */
    protected APIWrapper(final T api, final String apiName) {
        m_api = api;
        m_apiName = apiName;
    }

    /**
     * Functional interface used as parameter of the {@link APIWrapper#invoke(Invoker)} method.
     *
     * @param <R> type of return value
     */
    @FunctionalInterface
    protected interface Invoker<R> {
        R invoke() throws IOException;
    }

    /**
     * Helper to wrap method calls on the API.
     *
     * @param <R> type of return value
     * @param invoker runnable to invoke
     * @return value returned by invoker
     * @throws IOException
     */
    protected <R> R invoke(final Invoker<R> invoker) throws IOException {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
                return invoker.invoke();
            } catch (final DatabricksRateLimitClientErrorException e) { // NOSONAR
                waitBeforeRetry(i, e.getRetryAfter().orElse(null));
            } catch (final DatabricksRateLimitException e) { // NOSONAR
                waitBeforeRetry(i, e.getRetryAfter().orElse(null));
            }
        }

        throw new IOException("To many retries, Databrick request failed with rate limit."); // shoud never be reached
    }

    /**
     * Wait some time before doing a request retry.
     *
     * @param retryRound the round of the request, starting with {@code 0} on the first request
     * @param retryAfter duration from retry after response header, or {@code null} if not present in response
     * @throws IOException if thread was interrupted while waiting
     */
    private void waitBeforeRetry(final int retryRound, final Duration retryAfter) throws IOException {
        final Duration wait = waitTime(retryRound, retryAfter);

        if (retryRound % 10 == 0) {
            m_logger.debug(String.format(
                "Databricks REST API returned rate-limit error, waiting %d seconds before next retry."
                    + " (retry: %d, API: %s)",
                wait.getSeconds(), retryRound + 1, m_apiName));
        }

        try {
            Thread.sleep(wait.toMillis());
        } catch (final InterruptedException ex) { // NOSONAR rethrowing the exception
            throw new IOException("Databricks REST request interrupted while waiting for retry.", ex);
        }
    }

    /**
     * Calculate the next wait time using the duration from the retry-after response header, or exponential backoff time
     * if header not present.
     *
     * @param retryRound the round of the request, starting with {@code 0} on the first request
     * @param retryAfter duration from retry after response header, or {@code null} if not present in response
     * @throws IOException if thread was interrupted while waiting
     */
    private static Duration waitTime(final int round, final Duration retryAfter) {
        if (retryAfter != null) {
            return retryAfter;
        }

        if (round == 0) {
            return Duration.ofSeconds(1);
        }

        final long waitTime = (long)Math.pow(2, round);
        return Duration.ofSeconds(Math.min(MAX_WAIT_TIME, waitTime));
    }

    /**
     * @return the wrapped api
     */
    T getWrappedAPI() {
        return m_api;
    }

}
