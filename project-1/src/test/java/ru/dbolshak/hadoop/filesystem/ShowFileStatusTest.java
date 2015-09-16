package ru.dbolshak.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

/**
 * This classes shows a basic function of FileSystem in Hadoop
 */
public class ShowFileStatusTest {
    private static final String TEST_BUILD_DATA_DIR_PROPERTY = "test.build.data";
    private static final String TMP_DIR = "/tmp";

    private static final String DIR_PATH = "/dir";
    private static final String FILE_PATH = DIR_PATH + "/file";
    private static final String FILE_CONTENT = "content";
    private static final String FILE_ENCODING = "UTF-8";

    private static final String PERMISSIONS_FOR_DIR = "rwxr-xr-x";
    private static final String PERMISSIONS_FOR_FILE = PERMISSIONS_FOR_DIR.replace("x", "-");
    private static final String USER_NAME_PROPERTY = "user.name";
    private static final String USERNAME = "supergroup";

    private static final Long FILE_LEN = (long) FILE_CONTENT.length();
    private static final Long DFS_BLOCK_SIZE = (long) (128 * 1024 * 1024);
    private static final Short REPLICATION_FACTOR = 1;
    private static final TestingProperties FILE_PROPERTIES = new TestingProperties(FILE_LEN, DFS_BLOCK_SIZE, REPLICATION_FACTOR, PERMISSIONS_FOR_FILE);
    private static final TestingProperties DIR_PROPERTIES = new TestingProperties(0L, 0L, (short)0, PERMISSIONS_FOR_DIR);

    private MiniDFSCluster cluster;
    private FileSystem fs;

    private static int getFileLenAsInteger() {
        return Integer.valueOf(String.valueOf(FILE_LEN));
    }

    @Before
    public void setUp() throws IOException {
        Configuration conf = prepareConfiguration();
        buildClusterAndFileSystem(conf);
        createFile();
    }

    private Configuration prepareConfiguration() {
        Configuration conf = new Configuration();
        if (System.getProperty(TEST_BUILD_DATA_DIR_PROPERTY) == null) {
            System.setProperty(TEST_BUILD_DATA_DIR_PROPERTY, TMP_DIR);
        }
        return conf;
    }

    private void buildClusterAndFileSystem(Configuration conf) throws IOException {
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();
    }

    private void createFile() throws IOException {
        try (OutputStream out = fs.create(new Path(FILE_PATH))) {
            out.write(FILE_CONTENT.getBytes(FILE_ENCODING));
        }
    }

    @After
    public void shutDownCluster() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void testFileStatusForFileAndDirectory() throws IOException {
        for (String path : Arrays.asList(FILE_PATH, DIR_PATH)) {
            testFileSystemEntry(path);
        }
    }

    @Test
    public void readFile() throws IOException {
        Path file = new Path(FILE_PATH);
        FSDataInputStream inputStream = fs.open(file);
        byte[] buffer = new byte[Integer.valueOf(String.valueOf(FILE_LEN))];
        int receivedBytes = inputStream.read(buffer);
        String fileContent = new String(buffer, FILE_ENCODING);

        assertThat(receivedBytes, is(getFileLenAsInteger()));
        assertThat(fileContent, is(FILE_CONTENT));
    }

    private void testFileSystemEntry(String path) throws IOException {
        //noinspection StringEquality
        boolean isDir = DIR_PATH == path;

        TestingProperties testingProperties = isDir ? DIR_PROPERTIES : FILE_PROPERTIES;
        FileStatus stat = getFileStatus(path);

        assertThat(stat.isDirectory(), is(isDir));
        assertThat(stat.getLen(), is(testingProperties.getFileLen()));
        assertThat(stat.getBlockSize(), is(testingProperties.getDfsBlockSize()));
        assertThat(stat.getReplication(), is(testingProperties.getReplicationFactor()));
        assertThat(stat.getPermission().toString(), is(testingProperties.getPermissions()));

        assertThat(stat.getPath().toUri().getPath(), is(path));
        assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(stat.getOwner(), is(System.getProperty(USER_NAME_PROPERTY)));
        assertThat(stat.getGroup(), is(USERNAME));
    }

    private FileStatus getFileStatus(String path) throws IOException {
        Path file = new Path(path);
        return fs.getFileStatus(file);
    }

    private static class TestingProperties {
        private final long fileLen;
        private final long dfsBlockSize;
        private final short replicationFactor;
        private final String permissions;


        TestingProperties(long fileLen, long dfsBlockSize, short replicationFactor, String permissions) {
            this.fileLen = fileLen;
            this.dfsBlockSize = dfsBlockSize;
            this.replicationFactor = replicationFactor;
            this.permissions = permissions;
        }

        long getFileLen() {
            return fileLen;
        }

        long getDfsBlockSize() {
            return dfsBlockSize;
        }

        short getReplicationFactor() {
            return replicationFactor;
        }

        String getPermissions() {
            return permissions;
        }
    }
}
