package Chapter_2Two;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test_2Two {

    public static final String HDFS_PATH = "hdfs://192.168.50.129:8020";

    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        System.out.println("HDFSApp.setUp()");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
    }

    @After
    public void tearDown() {
        fileSystem = null;
        configuration = null;
        System.out.print("HDFSApp.tearDown()");
    }

    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void create() throws IOException {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello world".getBytes());

        output.flush();
        output.close();
    }

    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("hdfsapi/test/b.txt");
        System.out.println(fileSystem.rename(oldPath, newPath));
    }

    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("/home/hadoop/data/hello.txt");
        Path dist = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dist);
    }

    @Test
    public void listFiles() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("hdfsapi/test"));
        for (FileStatus fileStatus : listStatus) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("hdfsapi/test/b.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String host : block.getHosts()) {
                System.out.println(host);
            }
        }
    }

}
