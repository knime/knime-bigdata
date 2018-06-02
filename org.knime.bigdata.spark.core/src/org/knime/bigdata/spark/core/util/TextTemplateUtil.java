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
 *   Created on May 25, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Utility class to fill out a text template with values from a map.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class TextTemplateUtil {

    /**
     * Reads the given input stream into a template string and then replaces patterns in the template string with values
     * from the given map.
     *
     * @param templateStream A stream from which to read the template string (UTF-8 encoding is assumed).
     * @param replacements Maps patterns to replacement strings.
     * @return a filled out generated template.
     */
    public static String fillOutTemplate(final InputStream templateStream, final Map<String, String> replacements) throws IOException {
        final byte[] bytes = new byte[templateStream.available()];
        templateStream.read(bytes);
        final String templateString = new String(bytes, Charset.forName("UTF8"));

        return fillOutTemplate(templateString, replacements);
    }

    /**
     * Replaces patterns in the template string with values from the given map.
     *
     * @param templateString A string with the full template text.
     * @param replacements Maps patterns to replacement strings.
     * @return a filled out generated template.
     */
    public static String fillOutTemplate(final String templateString, final Map<String, String> replacements) {
        final StringBuilder buf = new StringBuilder(templateString);
        for (String pattern : replacements.keySet()) {
            replace(buf, pattern, replacements.get(pattern));
        }

        return buf.toString();
    }

    /**
     * Replaces all occurrences of the given pattern with the given value inside the given buffer.
     *
     * @param buf The buffer inside which to do the replacement.
     * @param pattern The pattern in the buffer to replace.
     * @param value The value to replace with.
     */
    protected static void replace(final StringBuilder buf, final String pattern, final String value) {
        final String realPattern = String.format("${%s}", pattern);
        boolean hasReplaced = false;

        int start;
        while ((start = buf.indexOf(realPattern)) != -1) {
            buf.replace(start, start + realPattern.length(), value);
            hasReplaced = true;
        }

        if (!hasReplaced) {
            throw new IllegalArgumentException(String.format("Pattern %s does not appear in template", realPattern));
        }
    }
}
