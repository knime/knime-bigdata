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
 *   Created on May 13, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.jobserver;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.util.Pair;

/**
 * This class caches information about which files have been uploaded to the Spark jobserver. A cache entry is a mapping
 * from a local filename to a pair, that contains the file's modification time and the corresponding filename on the
 * Spark jobserver.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class UploadFileCache {

    private final Map<String, Pair<Long, String>> m_inputFileCopyCache = new HashMap<>();

    /**
     * Adds cache entries for the given local/server files. If there is an existing cache entry for a local file, the
     * entry is replaced if and only if the modification time of given file is higher than the one in the cache entry.
     *
     * @param localFile
     * @param serverFile
     * @return true if a cache entry was added or an existing one replaced, false otherwise. In this case the
     *         server-side file can be deleted and the existing (fresher) mapping can be queried with
     *         {@link #tryToGetServerFileFromCache(File)}.
     */
    public synchronized boolean addFilesToCache(final File localFile, final String serverFile) {

        Pair<Long, String> newCacheEntry = new Pair<Long, String>(localFile.lastModified(), serverFile);
        Pair<Long, String> existingCacheEntry = m_inputFileCopyCache.get(localFile.getAbsolutePath());

        if (existingCacheEntry == null || newCacheEntry.getFirst() > existingCacheEntry.getFirst()) {
            m_inputFileCopyCache.put(localFile.getAbsolutePath(), newCacheEntry);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Checks whether there is an existing cache entry for the given file with the exact modification time of the file.
     *
     * @param file
     * @return true if a matching cache entry was found, false otherwise.
     */
    public String tryToGetServerFileFromCache(final File file) {
        String inputFilePath = file.getAbsolutePath();
        if (m_inputFileCopyCache.containsKey(inputFilePath)) {
            Pair<Long, String> mtimeAndServerPath = m_inputFileCopyCache.get(inputFilePath);

            final long cachedModificationTime = mtimeAndServerPath.getFirst();
            if (file.lastModified() == cachedModificationTime) {
                return mtimeAndServerPath.getSecond();
            }
        }

        return null;
    }

}
