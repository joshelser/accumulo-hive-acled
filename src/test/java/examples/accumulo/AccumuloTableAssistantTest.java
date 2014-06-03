package examples.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccumuloTableAssistantTest {

  private AccumuloTableAssistant assistant;
  private Configuration conf = new Configuration();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    assistant = new AccumuloTableAssistant.Builder().setInstanceName("test").setPassword("password").setUser("root").setTableName("blah")
        .setZooQuorum("localhost:2181").setUseMini(true).setTempDir(tmpDir.newFolder().getAbsolutePath()).build();
  }

  @After
  public void teardown() throws Exception {
    if (null != assistant) {
      assistant.close();
    }
  }

  @Test
  public void tableCreateAndDelete() {
    try {
      assistant.createTableIfNotExists();
      assertTrue(assistant.tableExists());
      assistant.deleteTableIfExists();
      assertTrue(!assistant.tableExists());
    } catch (AccumuloException e) {
      fail("Create table failed" + e.getMessage());
    } catch (AccumuloSecurityException e) {
      fail("Create table failed" + e.getMessage());
    }
  }

  @Test
  public void presplit() {
    try {
      File localSplitFile = new File(System.getProperty("user.dir"), "/target/splits.txt");
      FileWriter splitWriter = new FileWriter(localSplitFile);
      splitWriter.write("000001_zzzzz\n");
      splitWriter.write("999999_zzzzz");
      splitWriter.close();
      Path path = new Path("/tmp/splits.txt");
      assistant.createTableIfNotExists();
      int numSplits = assistant.presplitAndWriteHDFSFile(conf, localSplitFile.getAbsolutePath(), path.toUri().toString());
      assertEquals(numSplits, 2);

      Collection<Text> splitsFromTableOpt = assistant.getTableOpts().listSplits("blah");
      assertEquals(splitsFromTableOpt.size(), numSplits);
      assistant.deleteTableIfExists();
      assertFalse(assistant.tableExists());

      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream in = fs.open(path);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = reader.readLine()) != null) {
        line = new String(Base64.decodeBase64(line));
        assertEquals(line, "000001_zzzzz999999_zzzzz");
      }

    } catch (IOException e) {
      fail(e.getMessage());
    } catch (AccumuloException e) {
      fail("EXCEPTION fail: " + e.getMessage());
    } catch (AccumuloSecurityException e) {
      fail("EXCEPTION fail: " + e.getMessage());
    } catch (TableNotFoundException e) {
      fail("EXCEPTION fail: " + e.getMessage());
    }
  }
}
