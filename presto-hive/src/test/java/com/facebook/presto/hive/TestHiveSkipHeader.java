package com.facebook.presto.hive;

import com.facebook.presto.hive.benchmark.HiveFileFormatBenchmark;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.benchmark.FileFormat.HIVE_TEXTFILE;

public class TestHiveSkipHeader
{
  @Test
  public void testSkipHeader() throws IOException
  {
    File file = new File(this.getClass().getClassLoader().getResource("withHeader.csv").getPath());
    Properties skipHeader = new Properties();
    skipHeader.setProperty(serdeConstants.HEADER_COUNT, "1");
    skipHeader.setProperty(serdeConstants.FIELD_DELIM, ",");

    List<Page> pages = new ArrayList<>(100);

    try (ConnectorPageSource pageSource = HIVE_TEXTFILE.createFileFormatReader(
        HiveFileFormatBenchmark.SESSION,
        HiveFileFormatBenchmark.HDFS_ENVIRONMENT,
        file,
        skipHeader,
        ImmutableList.of("col1", "col2", "col3"),
        ImmutableList.of(VarcharType.VARCHAR, IntegerType.INTEGER, IntegerType.INTEGER)
    )) {
      while (!pageSource.isFinished()) {
        Page page = pageSource.getNextPage();
        if (page != null) {
          page.assureLoaded();
          pages.add(page);
        }
      }
    }

    Assert.assertEquals(1, pages.size());
    Page page = pages.get(0);

  }
}
