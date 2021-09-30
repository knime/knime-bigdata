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
package org.knime.bigdata.testing.node.create.utils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Utility to convert a path to a legacy Hadoop path usable in legacy nodes that do not support multiple windows drives.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LegacyHadoopPathUtil {

    private static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    private static final Pattern startsWithDriveLetterSpecifier = Pattern.compile("^/?[a-zA-Z]:");

    private LegacyHadoopPathUtil() {}

    /**
     * Remove the drive information from given path on windows and validate that the path uses the default drive.
     *
     * @param path the path to convert with possible drive letter and colon in front
     * @return path without drive letter and colon
     * @throws IOException if path contains not the default drive on windows
     */
    public static String convertToLegacyPath(final String path) throws IOException {
        if (WINDOWS && startsWithDriveLetterSpecifier.matcher(path).find()) {
            final String defaultDrive = Paths.get("/").toAbsolutePath().toString().substring(0, 1);
            final int start = path.startsWith("/") ? 1 : 0;
            final String pathDrive = path.substring(start, start + 1);

            if (!pathDrive.equalsIgnoreCase(defaultDrive)) {
                throw new IOException("Given path does not use default windows drive, unable to convert: " + path);
            } else {
                return path.substring(start + 2);
            }
        } else {
            return path;
        }
    }

}
