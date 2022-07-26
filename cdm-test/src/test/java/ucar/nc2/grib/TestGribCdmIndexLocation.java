package ucar.nc2.grib;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thredds.featurecollection.FeatureCollectionConfig;
import thredds.featurecollection.FeatureCollectionType;
import thredds.inventory.CollectionUpdateType;
import thredds.inventory.s3.MFileS3;
import ucar.nc2.grib.collection.GribCdmIndex;
import ucar.nc2.grib.collection.GribCollectionImmutable;
import ucar.nc2.util.DiskCache2;
import ucar.unidata.util.StringUtil2;

@RunWith(Parameterized.class)
public class TestGribCdmIndexLocation {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DATA_DIR = "../grib/src/test/data/";
  private static final String INDEX_DIR = "../grib/src/test/data/index/";
  private static final String COLLECTION_NAME = "TestGribCdmIndexLocation";

  private static final String BUCKET = "cdms3:thredds-test-data";
  private static final String FRAGMENT = "#delimiter=/";

  private static final String S3_DIR_WITH_INDEX = BUCKET + "?" + "test-grib-index";
  private static final String S3_DIR_WITHOUT_INDEX = BUCKET + "?" + "test-grib-without-index";

  private static final String[] FILENAMES = {"radar_national.grib1", "cosmo-eu.grib2"};
  private static final String[] PARTITION_TYPES = {"file"}; // TODO time?
  // private static final String[] PARTITION_TYPES = {"none", "directory", "file", "all"}; // TODO time?
  // TODO file and directory with wantSubDirs is another case!

  @Parameterized.Parameters(name = "{0}, {1}")
  public static List<Object[]> getTestParameters() {
    final List<Object[]> testCases = new ArrayList<>();

    for (String filename : FILENAMES) {
      for (String partitionType : PARTITION_TYPES) {
        testCases.add(new String[] {filename, partitionType});
      }
    }

    return testCases;
  }

  private final String filename;
  private final String partitionType;

  private final FeatureCollectionType featureCollectionType;
  private final String filter;
  private final String collectionName;
  private final String indexFilename;

  public TestGribCdmIndexLocation(String filename, String partitionType) {
    this.filename = filename;
    this.partitionType = partitionType;

    this.featureCollectionType =
        filename.endsWith(".grib1") ? FeatureCollectionType.GRIB1 : FeatureCollectionType.GRIB2;
    final String fileEnding = filename.split("\\.")[1];
    this.filter = "/.*" + fileEnding;
    this.collectionName = COLLECTION_NAME + "_" + fileEnding + "_" + partitionType;
    this.indexFilename = collectionName + GribCdmIndex.NCX_SUFFIX;
  }

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

  @Before
  public void removeIndexFiles() throws IOException {
    removeS3File(S3_DIR_WITHOUT_INDEX + "/" + indexFilename);
    removeS3File(S3_DIR_WITHOUT_INDEX + "/" + filename + GribIndex.GBX9_IDX);
  }

  // Collection index tests for local files
  @Test
  public void shouldReturnUpdateNeededForLocalFile() throws IOException {
    useCache(false);

    copyToTempFolder(DATA_DIR + filename);
    copyToTempFolder(INDEX_DIR + indexFilename);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, tempFolder.getRoot() + filter, null, null, null, partitionType, null);

    for (CollectionUpdateType updateType : CollectionUpdateType.values()) {
      final boolean updateNeeded = GribCdmIndex.updateGribCollection(config, updateType, logger);
      assertWithMessage("Unexpected result for CollectionUpdateType = " + updateType).that(updateNeeded)
          .isEqualTo(updateType == CollectionUpdateType.always);
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    copyToTempFolder(DATA_DIR + filename);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, tempFolder.getRoot() + filter, null, null, null, partitionType, null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();

      if (partitionType.equals("file")) {
        assertTempFolderHas(filename + GribCdmIndex.NCX_SUFFIX);
      }
      assertTempFolderHas(filename);
    }
  }

  @Test
  public void shouldUseGribCdmIndexInLocationOfDataFile() throws IOException {
    useCache(false);

    copyToTempFolder(DATA_DIR + filename);
    copyToTempFolder(INDEX_DIR + indexFilename);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, tempFolder.getRoot() + filter, null, null, null, "file", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
      assertThat(gribCollection).isNotNull();
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInDefaultCache() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final File copiedFile = copyToTempFolder(DATA_DIR + filename);
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, tempFolder.getRoot() + filter, null, null, null, "file", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();
      assertTempFolderHasSize(1);
      assertThat(diskCache.getCacheFile(copiedFile.getPath() + GribCdmIndex.NCX_SUFFIX).exists()).isTrue();
    }
  }

  // Collection index tests for S3 files
  @Test
  public void shouldReturnUpdateNeededForS3File() throws IOException {
    useCache(false);

    final String specWithIndex = S3_DIR_WITH_INDEX + filter + FRAGMENT;
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, specWithIndex, null, null, null, partitionType, null);

    CollectionUpdateType[] collectionUpdateTypes = new CollectionUpdateType[] {CollectionUpdateType.test, // TODO fails
                                                                                                          // for
                                                                                                          // directory
                                                                                                          // and file
                                                                                                          // (DirectoryCollection
                                                                                                          // gets used)
        CollectionUpdateType.testIndexOnly, // TODO fails for type==directory
        CollectionUpdateType.never, CollectionUpdateType.nocheck, CollectionUpdateType.always // TODO fails for
                                                                                              // directory and file
    };

    for (CollectionUpdateType updateType : collectionUpdateTypes) {
      final boolean updateNeeded = GribCdmIndex.updateGribCollection(config, updateType, logger);
      assertWithMessage("Unexpected result for CollectionUpdateType = " + updateType).that(updateNeeded)
          .isEqualTo(updateType == CollectionUpdateType.always);
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInS3() throws IOException {
    useCache(false);

    final String specWithIndex = S3_DIR_WITHOUT_INDEX + filter + FRAGMENT;
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, specWithIndex, null, null, null, partitionType, null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();
    }

    final MFileS3 s3FileIndex = new MFileS3(S3_DIR_WITHOUT_INDEX + "/" + indexFilename);
    assertThat(s3FileIndex.exists()).isTrue();
  }

  @Test
  public void shouldUseGribCdmIndexInS3() throws IOException {
    useCache(false);

    final String specWithIndex = S3_DIR_WITH_INDEX + filter + FRAGMENT;
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, specWithIndex, null, null, null, partitionType, null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
      assertThat(gribCollection).isNotNull();
    }
  }

  @Test
  public void shouldUseGribCdmIndexInS3TopLevel() throws IOException {
    useCache(false);

    final String topLevelSpecWithIndex = BUCKET + "?" + filename + FRAGMENT;
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, topLevelSpecWithIndex, null, null, null, partitionType, null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.never, logger)) {
      assertThat(gribCollection).isNotNull();
    }
  }

  @Test
  public void shouldCreateGribCdmIndexInDefaultCacheForS3File() throws IOException {
    final DiskCache2 diskCache = useCache(true);

    final String specWithoutIndex = S3_DIR_WITHOUT_INDEX + filter + FRAGMENT;
    final FeatureCollectionConfig config = new FeatureCollectionConfig(collectionName, "test/" + collectionName,
        featureCollectionType, specWithoutIndex, null, null, null, "all", null);

    try (GribCollectionImmutable gribCollection =
        GribCdmIndex.openGribCollection(config, CollectionUpdateType.always, logger)) {
      assertThat(gribCollection).isNotNull();

      String cachePath = S3_DIR_WITHOUT_INDEX + "/";
      cachePath = StringUtil2.remove(cachePath, ':');
      cachePath = StringUtil2.remove(cachePath, '?');
      cachePath = StringUtil2.remove(cachePath, '=');
      assertThat(diskCache.getCacheFile(cachePath + collectionName + GribCdmIndex.NCX_SUFFIX).exists()).isTrue();
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

  private void removeS3File(String filename) throws IOException {
    final MFileS3 s3FileIndex = new MFileS3(filename);

    if (s3FileIndex.exists()) {
      assertThat(s3FileIndex.delete()).isTrue();
    }

    assertThat(s3FileIndex.exists()).isFalse();
  }
}
