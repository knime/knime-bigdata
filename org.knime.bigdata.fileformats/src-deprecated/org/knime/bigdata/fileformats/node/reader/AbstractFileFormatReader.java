/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.node.reader;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileHandlingUtility;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.cloud.aws.s3.filehandler.S3RemoteFileHandler;
import org.knime.cloud.aws.sdkv2.util.AWSCredentialHelper;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.node.ExecutionContext;
import org.knime.core.util.FileUtil;
import org.knime.datatype.mapping.ExternalDataTableSpec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

/**
 * Abstract class for readers of BigData file formats
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractFileFormatReader {

    /**
     * Checks whether the schema match.
     *
     * @param schemas
     *            the schemas
     * @throws BigDataFileFormatException
     *             thrown if schemas do not match
     */
    protected static void checkSchemas(final List<DataTableSpec> schemas) {
        final DataTableSpec refSchema = schemas.get(0);
        for (int i = 1; i < schemas.size(); i++) {
            if (!schemas.get(i).equals(refSchema)) {
                throw new BigDataFileFormatException("Schemas of input files do not match.");
            }
        }
    }

    /**
     * Decides if the file should be downloaded and then read from the local copy.
     *
     * @param remoteFile the remote file to access
     * @return {@code true} if file should be downloaded and read from local copy
     * @throws IOException on missing connection informations
     */
    protected static boolean readFromLocalCopy(final RemoteFile<Connection> remoteFile) throws IOException {
        final ConnectionInformation conInfo = remoteFile.getConnectionInformation();

        if (conInfo == null
                || HDFSRemoteFileHandler.isSupportedConnection(conInfo)
                || HDFSLocalRemoteFileHandler.isSupportedConnection(conInfo)
                || S3RemoteFileHandler.PROTOCOL.getName().equals(conInfo.getProtocol())) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Adapts a given HDFS configuration based on the remote connection
     * @param remotefile the remote file to access
     * @return a Hadoop config
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws IOException
     */
    protected static Configuration createConfig(final RemoteFile<Connection> remotefile)
        throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException, IOException {

        final ConnectionInformation conInfo = remotefile.getConnectionInformation();
        // this works for: local HDFS, HDFS with simple auth
        Configuration toReturn = ConfigurationFactory.createBaseConfigurationWithSimpleAuth();

        if (conInfo != null) {

            if (HDFSRemoteFileHandler.isSupportedConnection(conInfo)
                || HDFSLocalRemoteFileHandler.isSupportedConnection(conInfo)) {

                if (conInfo.useKerberos()) {
                    toReturn = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
                }
            } else if (S3RemoteFileHandler.PROTOCOL.getName().equals(conInfo.getProtocol())) {
                if (!(conInfo instanceof CloudConnectionInformation)) {
                    throw new IllegalStateException("Please reset and execute the preceding S3 Connection node.");
                }
                final CloudConnectionInformation cloudConInfo =
                    (CloudConnectionInformation)remotefile.getConnectionInformation();

                final String accessID;
                final String secretKey;

                var awsProvider = AWSCredentialHelper.getCredentialProvider(cloudConInfo, "KNIME_S3_Connection");
                var awsCredentials = awsProvider.resolveCredentials();

                if (awsCredentials instanceof AwsBasicCredentials) {
                    accessID = awsCredentials.accessKeyId();
                    secretKey = awsCredentials.secretAccessKey();
                } else {
                    throw new IOException("This node only supports access key & secret for AWS authentication.");
                }

                toReturn.set("fs.s3n.awsAccessKeyId", accessID);
                toReturn.set("fs.s3n.awsSecretAccessKey", secretKey);
                toReturn.set("fs.s3.awsAccessKeyId", accessID);
                toReturn.set("fs.s3.awsSecretAccessKey", secretKey);
            }
        }

        return toReturn;
    }

    /**
     * Generates a S3n path for a S3 remote cloud file
     * @param cloudcon the cloud remote file
     * @return returns the Path wit s3n protocol
     */
    protected static Path generateS3nPath(final CloudRemoteFile<Connection> cloudcon) {
        try {
            return new Path(new URI("s3n", cloudcon.getContainerName(), "/" + cloudcon.getBlobName(), null));
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    private final RemoteFile<Connection> m_file;

    private DataTableSpec m_tableSpec;

    private final ExecutionContext m_exec;

    private final FileStoreFactory m_fileStoreFactory;

    private ExternalDataTableSpec<?> m_externalTableSpec;

    private final boolean m_useKerberos;

    /**
     * Download directory for local copies or {@code null} on HDFS/S3 connections.
     */
    private File m_localDownloadDir;

    /**
     * Download file count, used for temporary file suffix.
     */
    private int m_localDownloadCount;

    /**
     * Constructor for FileFormatReader
     *
     * @param file
     *            the file or directory to read from
     * @param exec
     *            the execution context
     * @param useKerberos
     */
    public AbstractFileFormatReader(final RemoteFile<Connection> file, final ExecutionContext exec, final boolean useKerberos) {

        m_exec = exec;
        m_file = file;
        m_fileStoreFactory = FileStoreFactory.createFileStoreFactory(exec);
        m_useKerberos = useKerberos;

    }

    /**
     * Creates a reader instance
     * @param schemas
     *            the list to store the table spec
     * @param remotefile
     *            the remote file to read
     */
    protected abstract void createReader(List<DataTableSpec> schemas, RemoteFile<Connection> remotefile);

    /**
     * @return the execution context
     */
    public ExecutionContext getExec() {
        return m_exec;
    }

    /**
     * @return the {@link FileStoreFactory} for {@link DataCell} creation
     */
    public FileStoreFactory getFileStoreFactory() {
        return m_fileStoreFactory;
    }

    /**
     * @return the m_externalTableSpec
     */
    public ExternalDataTableSpec<?> getexternalTableSpec() {
        return m_externalTableSpec;
    }

    /**
     * @return the source file/folder
     */
    public RemoteFile<Connection> getFile() {
        return m_file;
    }

    /**
     * Returns the iterator for the next file or null if no more files have to be
     * read.
     *
     * @param index
     *            the current index
     * @return the next FileFormatRowIterator or null if all files are read
     * @throws IOException
     *             thrown if files can not be read
     * @throws InterruptedException
     *             if file iterator throws Interrupted Exception
     * @throws Exception if files can not be read, if file iterator throws Interrupted Exception, if
     */
    public abstract FileFormatRowIterator getNextIterator(long index) throws Exception;

    /**
     * Get the table spec that was generated from the files informations.
     *
     * @return the table spec read from the file.
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * Initializes the file format reader. Checking the file schemas, setting a table spec and creating a reader.
     * @throws Exception
     */
    protected void init() throws Exception {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(SnappyCodec.class.getClassLoader());
            final List<DataTableSpec> schemas = new ArrayList<>();
            if (getFile().isDirectory()) {
                final RemoteFile<Connection>[] fileList = getFile().listFiles();
                if (fileList.length == 0) {
                    throw new BigDataFileFormatException("Empty directory.");
                }
                for (final RemoteFile<Connection> remotefile : fileList) {
                    final String filename = remotefile.getName();
                    if (remotefile.getSize() > 0 && !filename.startsWith(".") && !filename.startsWith("_")) {

                        createReader(schemas, remotefile);

                    }
                }
                if (!schemas.isEmpty()) {
                    checkSchemas(schemas);

                }
            } else {
                createReader(schemas, getFile());
            }
            setTableSpec(schemas.get(0));
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }

    }

    /**
     * Returns a RowIterator that iterates over the read DataRows.
     *
     * @return the RowIterator
     * @throws Exception
     *             thrown if reading errors occur
     */
    public RowIterator iterator() throws Exception {

        return new DirIterator(this);
    }

    /**
     * Sets the external table spec
     *
     * @param externalTableSpec
     *            the externalTableSpec to set
     */
    public void setExternalTableSpec(final ExternalDataTableSpec<?> externalTableSpec) {
        this.m_externalTableSpec = externalTableSpec;
    }

    /**
     * @param tableSpec
     *            the DataTableSpec to set
     */
    public void setTableSpec(final DataTableSpec tableSpec) {
        m_tableSpec = tableSpec;
    }

    /**
     * @return the useKerberos
     */
    public boolean useKerberos() {
        return m_useKerberos;
    }

    /**
     * Download given remote file to a local temporary file.
     *
     * @param remoteFile remote file to download
     * @return local temporary copy of remote file
     * @throws Exception on download failures
     */
    protected synchronized RemoteFile<Connection> downloadLocalCopy(final RemoteFile<Connection> remoteFile) throws Exception {
        if (m_localDownloadDir == null) {
            m_localDownloadDir = FileUtil.createTempDir("clouddownload");
            m_localDownloadCount = 0;
        }

        if (useKerberos()) {
            return UserGroupUtil.runWithProxyUserUGIIfNecessary(
                ugi -> ugi.doAs((PrivilegedExceptionAction<RemoteFile<Connection>>)() -> downloadLocalCopy(m_exec,
                    remoteFile, m_localDownloadDir, m_localDownloadCount++)));
        } else {
            return downloadLocalCopy(m_exec, remoteFile, m_localDownloadDir, m_localDownloadCount++);
        }
    }

    private static RemoteFile<Connection> downloadLocalCopy(final ExecutionContext exec,
        final RemoteFile<Connection> remotefile, final File localDownloadDir, final int fileCount) throws Exception {

        final RemoteFile<Connection> tempFile = FileHandlingUtility.createTempFile(localDownloadDir, fileCount);
        tempFile.write(remotefile, exec);
        return tempFile;
    }
}
