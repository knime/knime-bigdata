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
 *   Nov 2, 2020 (Bjoern Lohrmann, KNIME GmbH): created
 */
package org.knime.bigdata.hadoop.filehandling.knox.fs;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.filehandling.knox.rest.KnoxAuthenticationException;
import org.knime.bigdata.filehandling.knox.rest.RemoteException;
import org.knime.bigdata.filehandling.knox.rest.WebHDFSAPI;
import org.knime.filehandling.core.defaultnodesettings.ExceptionUtil;

/**
 * Utlility class to map exceptions thrown by {@link WebHDFSAPI} client to {@link IOException}s.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public final class ExceptionMapper {

    private ExceptionMapper() {
    }

    /**
     * Maps the exception thrown by {@link WebHDFSAPI} client to an {@link IOException}.
     *
     * @param e The exception that was thrown.
     * @param path The affected file, must not be null.
     * @param otherPath The other affected file, may be null;
     * @return The exception to throw instead.
     */
    @SuppressWarnings("resource")
    public static IOException mapException(final Exception e, final KnoxHdfsPath path, final KnoxHdfsPath otherPath) {
        String message = null;
        if (e instanceof ClientErrorException) {
            message = extractMessage(((ClientErrorException)e).getResponse());
        }

        final String file = path.toString();
        final String otherFile = otherPath != null ? otherPath.toString() : null;

        final IOException toReturn;

        if (e instanceof NotAuthorizedException) {
            toReturn = new KnoxAuthenticationException(makeMessage(message, "Authentication failed. Please check username and password."));
        } else if (e instanceof ForbiddenException) {
            toReturn = new AccessDeniedException(file, otherFile, makeMessage(message, "Access denied"));
        } else if (e instanceof NotFoundException) {
            toReturn = new NoSuchFileException(file);
        } else {
            toReturn = ExceptionUtil.wrapAsIOException(e);
        }

        return toReturn;
    }

    private static String makeMessage(final String serverProvidedMessage, final String defaultMessage) {
        if (!StringUtils.isBlank(serverProvidedMessage)) {
            return serverProvidedMessage;
        } else {
            return defaultMessage;
        }
    }

    private static String extractMessage(final Response response) {
        String message = null;

        // try to parse remote exceptions
        if (response != null && response.getMediaType() != null
            && response.getMediaType().getSubtype().toLowerCase().contains("json")) {

            try {
                final RemoteException remoteException = response.readEntity(RemoteException.class);
                if (!StringUtils.isBlank(remoteException.message)) {
                    message = remoteException.message;
                } else {
                    message = response.getStatusInfo().getReasonPhrase();
                }
            } catch (Exception e) { // NOSONAR
                message = e.getMessage();
            }
        }
        return message;
    }

}
