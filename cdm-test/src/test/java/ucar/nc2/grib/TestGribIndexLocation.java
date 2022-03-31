package ucar.nc2.grib;

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thredds.filesystem.MFileOS;
import thredds.inventory.CollectionUpdateType;
import thredds.inventory.MFile;
import thredds.inventory.s3.MFileS3;
import ucar.nc2.util.DiskCache2;

@RunWith(Parameterized.class)
public class TestGribIndexLocation {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DATA_DIR = "../grib/src/test/data/";
  private static final String INDEX_DIR = "../grib/src/test/data/index/";

  private static final String BUCKET = "cdms3:thredds-test-data";
  private static final String S3_DIR_WITH_INDEX = BUCKET + "?" + "test-grib-index/";
  private static final String S3_DIR_WITHOUT_INDEX = BUCKET + "?" + "test-grib-without-index/";

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {

        {"radar_national.grib1"},

        {"cosmo-eu.grib2"},});
  }

  private final String filename;
  private final String indexFilename;
  private final boolean isGrib1;

  public TestGribIndexLocation(String filename) {
    this.filename = filename;
    this.indexFilename = filename + GribIndex.GBX9_IDX;
    this.isGrib1 = filename.endsWith(".grib1");
  }

  @Rule
  public final TemporaryFolder tempCacheFolder = new TemporaryFolder();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setCacheLocation() {
    System.getProperties().setProperty("nj22.cache", tempCacheFolder.getRoot().getPath());
  }

  @After
  public void unsetCacheLocation() {
    System.clearProperty("nj22.cache");
  }

  @Before
  public void removeIndexFiles() throws IOException {
    final MFileS3 s3FileIndex = new MFileS3(S3_DIR_WITHOUT_INDEX + indexFilename);

    if (s3FileIndex.exists()) {
      assertThat(s3FileIndex.delete()).isTrue();
    }

    assertThat(s3FileIndex.exists()).isFalse();
  }

  // Grib index tests for local files
  @Test
  public void shouldCreateIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    final MFile copiedFile = new MFileOS(copyToTempFolder(DATA_DIR + filename).getPath());

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, copiedFile, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);

    assertTempFolderHasSize(2);
    assertTempFolderHas(copiedFile.getName() + GribIndex.GBX9_IDX);
  }

  @Test
  public void shouldUseIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    final MFile copiedFile = new MFileOS(copyToTempFolder(DATA_DIR + filename).getPath());
    final File copiedIndexFile = copyToTempFolder(INDEX_DIR + indexFilename);
    assertThat(copiedIndexFile.setLastModified(0)).isTrue();

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, copiedFile, CollectionUpdateType.nocheck, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);

    assertTempFolderHasSize(2);
    assertTempFolderHas(copiedFile.getName() + GribIndex.GBX9_IDX);
    assertThat(copiedIndexFile.lastModified()).isEqualTo(0);
  }

  @Test
  public void shouldCreateIndexInDefaultCache() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final MFile copiedFile = new MFileOS(copyToTempFolder(DATA_DIR + filename).getPath());

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, copiedFile, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);

    assertTempFolderHasSize(1);
    assertThat(diskCache.getCacheFile(copiedFile.getPath() + GribIndex.GBX9_IDX).exists()).isTrue();
  }

  // Grib index tests for S3 files
  @Test
  public void shouldCreateIndexInS3() throws IOException {
    useCache(false);

    final MFile s3File = new MFileS3(S3_DIR_WITHOUT_INDEX + filename);

    // Check that index file does not exist
    MFile s3FileIndex = new MFileS3(S3_DIR_WITHOUT_INDEX + indexFilename);
    assertThat(s3FileIndex.exists()).isFalse();

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, s3File, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);

    // Need a new MFileS3, since it stores "exists" instead of checking everytime
    s3FileIndex = new MFileS3(S3_DIR_WITHOUT_INDEX + indexFilename);
    assertThat(s3FileIndex.exists()).isTrue();
    assertThat(s3FileIndex.delete()).isTrue();
  }

  @Test
  public void shouldUseIndexInS3() throws IOException {
    useCache(false);

    final MFile s3File = new MFileS3(S3_DIR_WITH_INDEX + filename);

    // Check that index file exists
    final MFile s3FileIndex = new MFileS3(S3_DIR_WITH_INDEX + indexFilename);
    assertThat(s3FileIndex.exists()).isTrue();

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, s3File, CollectionUpdateType.nocheck, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);
  }

  @Test
  public void shouldCreateIndexInDefaultCacheForS3File() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final MFile s3File = new MFileS3(S3_DIR_WITHOUT_INDEX + filename);

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(isGrib1, s3File, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertThat(index.getNRecords()).isNotEqualTo(0);

    assertThat(diskCache.getCacheFile(s3File.getPath() + GribIndex.GBX9_IDX).exists()).isTrue();
  }

  // Helper functions
  private DiskCache2 useCache(boolean useCache) {
    final DiskCache2 diskCache = GribIndexCache.getDiskCache2();
    diskCache.setNeverUseCache(!useCache);
    diskCache.setAlwaysUseCache(useCache);
    return diskCache;
  }

  private File copyToTempFolder(String filename) throws IOException {
    final File file = new File(filename);
    final File copiedFile = new File(tempFolder.getRoot(), file.getName());
    Files.copy(file.toPath(), copiedFile.toPath());
    assertTempFolderHas(copiedFile.getName());
    return copiedFile;
  }

  private void assertTempFolderHas(String filename) {
    final File[] filesInFolder = tempFolder.getRoot().listFiles();
    assertThat(filesInFolder).isNotNull();
    assertThat(Arrays.stream(filesInFolder).anyMatch(file -> file.getName().equals(filename))).isTrue();
  }

  private void assertTempFolderHasSize(int size) {
    final File[] filesInFolder = tempFolder.getRoot().listFiles();
    assertThat(filesInFolder).isNotNull();
    assertThat(filesInFolder.length).isEqualTo(size);
  }
}
