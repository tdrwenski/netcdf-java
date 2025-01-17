/*
 * (c) 1998-2018 John Caron and University Corporation for Atmospheric Research/Unidata
 */
package ucar.nc2.ft2.coverage;

import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.DatasetUrl;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.ft.FeatureDataset;
import ucar.nc2.ft2.coverage.adapter.DtCoverageAdapter;
import ucar.nc2.ft2.coverage.adapter.DtCoverageDataset;
import ucar.nc2.util.Optional;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

/**
 * factory for CoverageDataset
 * 1) Remote CdmrFeatureDataset: cdmremote:url
 * 2) GRIB collections: must be a grib file, or grib index file
 * 3) DtCoverageDataset (forked from ucar.nc2.dt.grid), the cdm IOSP / CoordSys stack
 * <p>
 * Would like to add a separate implementation for FMRC collections
 *
 * @author caron
 * @since 5/26/2015
 */
public class CoverageDatasetFactory {
  public static final String NOT_GRIB_FILE = "Not a GRIB file";
  public static final String NO_GRIB_CLASS = "GRIB module not loaded";

  /**
   * @param endpoint cdmrFeature:url, local GRIB data or index file, or NetcdfDataset location
   * 
   *        <pre>
   *        ucar.nc2.util.Optional<FeatureDatasetCoverage> opt = CoverageDatasetFactory.openCoverageDataset(location);
   *        if (!opt.isPresent()) {
   *          JOptionPane.showMessageDialog(null, opt.getErrorMessage());
   *          return false;
   *        }
   *        covDatasetCollection = opt.get();
   *        </pre>
   */
  public static Optional<FeatureDatasetCoverage> openCoverageDataset(String endpoint) throws IOException {

    // remote cdmrFeature datasets
    if (endpoint.startsWith(ucar.nc2.ft.remote.CdmrFeatureDataset.SCHEME)) {
      Optional<FeatureDataset> opt = ucar.nc2.ft.remote.CdmrFeatureDataset.factory(FeatureType.COVERAGE, endpoint);
      return opt.isPresent() ? Optional.of((FeatureDatasetCoverage) opt.get()) : Optional.empty(opt.getErrorMessage());
    }

    DatasetUrl durl = DatasetUrl.findDatasetUrl(endpoint);
    if (durl.serviceType == null) { // skip GRIB check for anything not a plain ole file
      // check if its GRIB collection
      Optional<FeatureDatasetCoverage> opt = openGrib(endpoint);
      if (opt.isPresent())
        return opt;
      if (opt.getErrorMessage() == null)
        return Optional.empty("Unknown error opening grib coverage dataset");
      if (!opt.getErrorMessage().startsWith(CoverageDatasetFactory.NOT_GRIB_FILE)
          && !opt.getErrorMessage().startsWith(CoverageDatasetFactory.NO_GRIB_CLASS)) {
        return opt; // its a GRIB file with an error
      }
    }

    // adapt a DtCoverageDataset (forked from ucar.nc2.dt.GridDataset), eg a local file
    DtCoverageDataset gds = DtCoverageDataset.open(durl);
    if (!gds.getGrids().isEmpty()) {
      Formatter errlog = new Formatter();
      FeatureDatasetCoverage result = DtCoverageAdapter.factory(gds, errlog);
      if (result != null)
        return Optional.of(result);
      else
        return Optional.empty(errlog.toString());
    }

    gds.close();
    return Optional.empty("Could not open as a coverage dataset");
  }

  /**
   *
   * @param endpoint cdmrFeature:url, local GRIB data or index file, or NetcdfDataset location
   * @return FeatureDatasetCoverage or null on failure. use openCoverageDataset to get error message
   */
  public static FeatureDatasetCoverage open(String endpoint) throws IOException {
    Optional<FeatureDatasetCoverage> opt = openCoverageDataset(endpoint);
    return opt.isPresent() ? opt.get() : null;
  }

  /**
   * @param endpoint local GRIB data or index file
   * @return FeatureDatasetCoverage or null on failure.
   */
  public static Optional<FeatureDatasetCoverage> openGrib(String endpoint) {

    List<Object> notGribThrowables = Arrays.asList(IllegalAccessException.class, IllegalArgumentException.class,
        ClassNotFoundException.class, NoSuchMethodException.class, NoSuchMethodError.class);

    try {
      Class<?> c =
          CoverageDatasetFactory.class.getClassLoader().loadClass("ucar.nc2.grib.coverage.GribCoverageDataset");
      Method method = c.getMethod("open", String.class);
      return (Optional<FeatureDatasetCoverage>) method.invoke(null, endpoint);
    } catch (Exception e) {
      for (Object noGrib : notGribThrowables) {
        // check for possible errors that are due to the file not being grib. Need to look
        // at the error causes too, as reflection error can be buried under a InvorcationTargetException
        boolean notGribTopLevel = e.getClass().equals(noGrib);
        boolean notGribBuried = e.getClass().equals(InvocationTargetException.class) && e.getCause() != null
            && e.getCause().getClass().equals(noGrib);

        if (notGribTopLevel || notGribBuried) {
          return Optional.empty(NO_GRIB_CLASS);
        }
      }
      // Ok, something went wrong, and it does not appear to be related to the file *not* being
      // a grib file.
      return Optional.empty(e.getCause().getMessage());
    }

  }

  /**
   * @deprecated Use openNcmlString(String, String)
   */
  @Deprecated
  public static Optional<FeatureDatasetCoverage> openNcmlString(String ncml) throws IOException {
    return openNcmlString(ncml, null);
  }

  /**
   * @param ncml the NcML as a String
   * @param location the URL location string of the NcML document,
   *        or may be just a unique name for caching purposes (if null, aggregation cache will not be used).
   */
  public static Optional<FeatureDatasetCoverage> openNcmlString(String ncml, String location) throws IOException {
    NetcdfDataset ncd = NetcdfDatasets.openNcmlDataset(new StringReader(ncml), location, null);

    DtCoverageDataset gds = new DtCoverageDataset(ncd);
    if (!gds.getGrids().isEmpty()) {
      Formatter errlog = new Formatter();
      FeatureDatasetCoverage cc = DtCoverageAdapter.factory(gds, errlog);
      return ucar.nc2.util.Optional.of(cc);
    }
    return Optional.empty("Could not open NcML as Coverage");
  }

}
