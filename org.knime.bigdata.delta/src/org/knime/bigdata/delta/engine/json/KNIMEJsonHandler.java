/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.knime.bigdata.delta.engine.json;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.knime.bigdata.delta.util.NioLogStore;
import org.knime.bigdata.delta.util.PathUtil;
import org.knime.filehandling.core.connections.FSPath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.defaults.internal.types.DataTypeParser;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;

/**
 * Default implementation of {@link JsonHandler} based on Hadoop APIs.
 */
public class KNIMEJsonHandler implements JsonHandler {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final ObjectReader defaultObjectReader = mapper.reader();

    // by default BigDecimals are truncated and read as floats
    private static final ObjectReader objectReaderReadBigDecimals =
        mapper.reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final Configuration hadoopConf;

    private final int maxBatchSize;

    private final FSPath m_rootPath;

    public KNIMEJsonHandler(final Configuration hadoopConf, final FSPath rootPath) {
        this.hadoopConf = hadoopConf;
        m_rootPath = rootPath;
        this.maxBatchSize =
            hadoopConf.getInt("delta.kernel.default.json.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid JSON reader batch size: " + maxBatchSize);
    }

    @Override
    public ColumnarBatch parseJson(final ColumnVector jsonStringVector, final StructType outputSchema,
        final Optional<ColumnVector> selectionVector) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < jsonStringVector.getSize(); i++) {
            boolean isSelected = !selectionVector.isPresent()
                || (!selectionVector.get().isNullAt(i) && selectionVector.get().getBoolean(i));
            if (isSelected && !jsonStringVector.isNullAt(i)) {
                rows.add(parseJson(jsonStringVector.getString(i), outputSchema));
            } else {
                rows.add(null);
            }
        }
        return new DefaultRowBasedColumnarBatch(outputSchema, rows);
    }

    @Override
    public StructType deserializeStructType(final String structTypeJson) {
        try {
            // We don't expect Java BigDecimal anywhere in a Delta schema so we use the default
            // JSON reader
            return DataTypeParser.parseSchema(defaultObjectReader.readTree(structTypeJson));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(format("Could not parse JSON: %s", structTypeJson), ex);
        }
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(final CloseableIterator<FileStatus> scanFileIter,
        final StructType physicalSchema, final Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private FileStatus currentFile;

            private BufferedReader currentFileReader;

            private String nextLine;

            @Override
            public void close() throws IOException {
                Utils.closeCloseables(currentFileReader, scanFileIter);
            }

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null || (nextLine = currentFileReader.readLine()) == null) {

                        tryOpenNextFile();
                        if (currentFileReader != null) {
                            nextLine = currentFileReader.readLine();
                        }
                    }
                } catch (IOException ex) {
                    throw new KernelException(format("Error reading JSON file: %s", currentFile.getPath()), ex);
                }

                return nextLine != null;
            }

            @Override
            public ColumnarBatch next() {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }

                List<Row> rows = new ArrayList<>();
                int currentBatchSize = 0;
                do {
                    // hasNext already reads the next one and keeps it in member variable `nextLine`
                    rows.add(parseJson(nextLine, physicalSchema));
                    nextLine = null;
                    currentBatchSize++;
                } while (currentBatchSize < maxBatchSize && hasNext());

                return new DefaultRowBasedColumnarBatch(physicalSchema, rows);
            }

            private void tryOpenNextFile() throws IOException {
                Utils.closeCloseables(currentFileReader); // close the current opened file
                currentFileReader = null;

                if (scanFileIter.hasNext()) {
                    currentFile = scanFileIter.next();
                    Path filePath = PathUtil.getHadoopPath(m_rootPath, hadoopConf, currentFile.getPath());
                    FileSystem fs = filePath.getFileSystem(hadoopConf);
                    FSDataInputStream stream = null;
                    try {
                        stream = fs.open(filePath);
                        currentFileReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
                        throw e;
                    }
                }
            }
        };
    }

    /**
     * Makes use of {@link LogStore} implementations in `delta-storage` to atomically write the data to a file depending
     * upon the destination filesystem.
     *
     * @param filePath Destination file path
     * @param data Data to write as Json
     * @throws IOException
     */
    @Override
    public void writeJsonFileAtomically(final String filePath, final CloseableIterator<Row> data,
        final boolean overwrite) throws IOException {
        Path path = PathUtil.getHadoopPath(m_rootPath, hadoopConf, filePath);
        //        LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());
        LogStore logStore = new NioLogStore(hadoopConf);
        try {
            logStore.write(path, new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return data.hasNext();
                }

                @Override
                public String next() {
                    return JsonUtils.rowToJson(data.next());
                }
            }, overwrite, hadoopConf);
        } finally {
            Utils.closeCloseables(data);
        }
    }

    private Row parseJson(final String json, final StructType readSchema) {
        try {
            final JsonNode jsonNode = objectReaderReadBigDecimals.readTree(json);
            return new DefaultJsonRow((ObjectNode)jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new KernelException(format("Could not parse JSON: %s", json), ex);
        }
    }
}
