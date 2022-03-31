package ucar.nc2.grib;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import thredds.featurecollection.FeatureCollectionConfig;
import thredds.featurecollection.FeatureCollectionType;
import thredds.filesystem.MFileOS;
import thredds.inventory.CollectionUpdateType;
import thredds.inventory.MFile;
import thredds.inventory.s3.MFileS3;
import ucar.nc2.grib.collection.GribCdmIndex;
import ucar.nc2.grib.collection.GribCollectionImmutable;
import ucar.nc2.util.DiskCache2;
import ucar.unidata.util.StringUtil2;

// TODO parametrize to also test grib2
public class TestGribIndexLocation {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String FILENAME = "../grib/src/test/data/radar_national.grib1";
  private static final String INDEX_FILENAME = "../grib/src/test/data/index/radar_national.grib1.gbx9";
  private static final String CDM_INDEX_FILENAME = "../grib/src/test/data/index/TestGribCdmIndexLocation.ncx4";

  private static final String BUCKET = "cdms3:thredds-test-data";
  private static final String DELIMITER = "#delimiter=/";
  private static final String FILTER = ".*grib1";

  private static final String DIR_WITH_INDEX = BUCKET + "?" + "test-grib-index/";
  private static final String KEY_WITH_INDEX = "radar_national.grib1";
  private static final String PATH_WITH_INDEX = DIR_WITH_INDEX + KEY_WITH_INDEX;
  private static final String SPEC_WITH_INDEX = DIR_WITH_INDEX + FILTER + DELIMITER;

  private static final String DIR_WITHOUT_INDEX = BUCKET + "?" + "test-grib-without-index/";
  private static final String KEY_WITHOUT_INDEX = "radar_national.grib1";
  private static final String PATH_WITHOUT_INDEX = DIR_WITHOUT_INDEX + KEY_WITHOUT_INDEX;
  private static final String SPEC_WITHOUT_INDEX = DIR_WITHOUT_INDEX + FILTER + DELIMITER;

  private static final String TOP_LEVEL_SPEC_WITH_INDEX = BUCKET + "?" + KEY_WITH_INDEX + DELIMITER;

  private static final boolean IS_GRIB1 = true;

  private static final String[] GRIB_TYPE_ENDINGS = {".grib1", ".grib2"};
  private static final String[] PARTITION_TYPES = {"none", "directory", "file", "all"}; // TODO time?
  private static final String COLLECTION_NAME = "TestGribCdmIndexLocation";

  @Rule
  public final TemporaryFolder tempCacheFolder = new TemporaryFolder();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void before() throws IOException {
    // This is not being used in the tests, but if unset gives a nullptr exception
    GribCdmIndex.setGribCollectionCache(new ucar.nc2.util.cache.FileCacheGuava("GribCollectionCacheGuava", 100));
  }

  @Before
  public void setCacheLocation() {
    System.getProperties().setProperty("nj22.cache", tempCacheFolder.getRoot().getPath());
  }

  @After
  public void unsetCacheLocation() {
    System.clearProperty("nj22.cache");
  }

  // Grib index tests for local files
  @Test
  public void shouldCreateIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    final MFile copiedFile = new MFileOS(copyToTempFolder(FILENAME).getPath());

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, copiedFile, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertTempFolderHasSize(2);
    assertTempFolderHas(copiedFile.getName() + GribIndex.GBX9_IDX);
  }

  @Test
  public void shouldUseIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    final MFile copiedFile = new MFileOS(copyToTempFolder(FILENAME).getPath());
    final File copiedIndexFile = copyToTempFolder(INDEX_FILENAME);
    assertThat(copiedIndexFile.setLastModified(0)).isTrue();

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, copiedFile, CollectionUpdateType.nocheck, logger);
    assertThat(index).isNotNull();
    assertTempFolderHasSize(2);
    assertTempFolderHas(copiedFile.getName() + GribIndex.GBX9_IDX);
    assertThat(copiedIndexFile.lastModified()).isEqualTo(0);
  }

  @Test
  public void shouldCreateIndexInDefaultCache() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final MFile copiedFile = new MFileOS(copyToTempFolder(FILENAME).getPath());

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, copiedFile, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
    assertTempFolderHasSize(1);

    assertThat(diskCache.getCacheFile(copiedFile.getPath() + GribIndex.GBX9_IDX).exists()).isTrue();
  }

  // Grib index tests for S3 files
  @Ignore("TODO need to have a way to create index files in s3, either with putObject or possibly as an output stream")
  @Test
  public void shouldCreateIndexInS3() throws IOException {
    useCache(false);

    final MFile s3File = new MFileS3(PATH_WITHOUT_INDEX);

    // Check that index file does not exist
    final MFile s3FileIndex = new MFileS3(PATH_WITHOUT_INDEX + GribIndex.GBX9_IDX);
    assertThrows("Expected index file to not exist", NoSuchKeyException.class, s3FileIndex::getLastModified);

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, s3File, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();

    // Check that index file exists
    assertThat(s3FileIndex.getLastModified()).isNotNull();

    // TODO remove index file or maybe use a temporary s3 bucket for testing
  }

  @Test
  public void shouldUseIndexInS3() throws IOException {
    useCache(false);

    final MFile s3File = new MFileS3(PATH_WITH_INDEX);

    // Check that index file exists
    final MFile s3FileIndex = new MFileS3(PATH_WITH_INDEX + GribIndex.GBX9_IDX);
    assertThat(s3FileIndex.getLastModified()).isNotNull();

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, s3File, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();
  }

  @Test
  public void shouldCreateIndexInDefaultCacheForS3File() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final MFile s3File = new MFileS3(PATH_WITHOUT_INDEX);

    final GribIndex index =
        GribIndex.readOrCreateIndexFromSingleFile(IS_GRIB1, s3File, CollectionUpdateType.always, logger);
    assertThat(index).isNotNull();

    assertThat(diskCache.getCacheFile(s3File.getPath() + GribIndex.GBX9_IDX).exists()).isTrue();
  }

  // Collection index tests for local files
  @Test
  public void shouldCreateGribCdmIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    final File copiedFile = copyToTempFolder(FILENAME);

    for (String partitionType : PARTITION_TYPES) {
      final String collectionName = COLLECTION_NAME + "_" + partitionType;
      final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
          FeatureCollectionType.GRIB1, tempFolder.getRoot() + "/.*grib1", null, null, null, partitionType, null);

      try (GribCollectionImmutable gribCollection =
          GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
        assertWithMessage("Partition type: " + partitionType).that(gribCollection).isNotNull();

        final File indexFile = new File(tempFolder.getRoot(), copiedFile.getName() + GribCdmIndex.NCX_SUFFIX);
        if (partitionType.equals("file")) {
          assertTempFolderHas(indexFile.getName());
        }

        final File collectionIndexFile = new File(tempFolder.getRoot(), collectionName + GribCdmIndex.NCX_SUFFIX);
        assertTempFolderHas(collectionIndexFile.getName());
      }
    }
  }

  @Test
  public void shouldUseGribCdmIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    copyToTempFolder(FILENAME);
    copyToTempFolder(CDM_INDEX_FILENAME);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(COLLECTION_NAME, "test/" + COLLECTION_NAME,
        FeatureCollectionType.GRIB1, tempFolder.getRoot() + "/.*grib1", null, null, null, "file", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
      assertThat(gribCollection).isNotNull();
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInDefaultCache() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final File copiedFile = copyToTempFolder(FILENAME);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(COLLECTION_NAME, "test/" + COLLECTION_NAME,
        FeatureCollectionType.GRIB1, tempFolder.getRoot() + "/.*grib1", null, null, null, "file", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();
      assertTempFolderHasSize(1);
      assertThat(diskCache.getCacheFile(copiedFile.getPath() + GribCdmIndex.NCX_SUFFIX).exists()).isTrue();
    }
  }

  // Collection index tests for S3 files
  // TODO test other "ptypes", at least for S3
  @Ignore("TODO need to have a way to create index files in s3, either with putObject or possibly as an output stream")
  @Test
  public void shouldCreateGribCdmIndexInS3() throws IOException {
    useCache(false);
    // TODO
  }

  // TODO test top level bucket (seems to be going into subdirs even with delimiter set, problem with iterator?)
  @Test
  public void shouldUseGribCdmIndexInS3() throws IOException {
    useCache(false);

    for (String partitionType: PARTITION_TYPES) {
      final String collectionName = COLLECTION_NAME + "_" + partitionType;
      final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
          FeatureCollectionType.GRIB1, SPEC_WITH_INDEX, null, null, null, partitionType, null);

      try (GribCollectionImmutable gribCollection =
          GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
        assertWithMessage("Null index with partition type: " + partitionType).that(gribCollection).isNotNull();
      }
    }
  }

  @Test
  public void shouldUseGribCdmIndexInS3TopLevel() throws IOException {
    useCache(false);

    for (String partitionType: new String[] {"file"}) {
      final String collectionName = COLLECTION_NAME + "_" + partitionType;
      final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
          FeatureCollectionType.GRIB1, TOP_LEVEL_SPEC_WITH_INDEX, null, null, null, partitionType, null);

      try (GribCollectionImmutable gribCollection =
          GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
        assertWithMessage("Null index with partition type: " + "file").that(gribCollection).isNotNull();
      }
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInDefaultCacheForS3File() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final FeatureCollectionConfig config = new FeatureCollectionConfig(COLLECTION_NAME, "test/" + COLLECTION_NAME,
        FeatureCollectionType.GRIB1, SPEC_WITHOUT_INDEX, null, null, null, "all", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();

      String cachePath = DIR_WITHOUT_INDEX;
      cachePath = StringUtil2.remove(cachePath, ':');
      cachePath = StringUtil2.remove(cachePath, '?');
      cachePath = StringUtil2.remove(cachePath, '=');
      assertThat(diskCache.getCacheFile(cachePath + COLLECTION_NAME + GribCdmIndex.NCX_SUFFIX).exists()).isTrue();
    }
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
