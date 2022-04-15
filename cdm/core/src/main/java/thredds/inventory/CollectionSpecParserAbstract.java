/*
 * Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package thredds.inventory;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import ucar.unidata.util.StringUtil2;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Formatter;

/**
 * Parses the collection specification string.
 * <p>
 * the idea is that one copies the full path of an example dataset, then edits it
 * </p>
 * <p>
 * Example: "/data/ldm/pub/native/grid/NCEP/GFS/Alaska_191km/** /GFS_Alaska_191km_#yyyyMMdd_HHmm#\.grib1$"
 * </p>
 * <ul>
 * <li>rootDir ="/data/ldm/pub/native/grid/NCEP/GFS/Alaska_191km"/</li>
 * <li>subdirs=true (because ** is present)</li>
 * <li>dateFormatMark="GFS_Alaska_191km_#yyyyMMdd_HHmm"</li>
 * <li>regExp='GFS_Alaska_191km_.............\.grib1$</li>
 * </ul>
 * <p>
 * Example: "Q:/grid/grib/grib1/data/agg/.*\.grb"
 * </p>
 * <ul>
 * <li>rootDir ="Q:/grid/grib/grib1/data/agg/"/</li>
 * <li>subdirs=false</li>
 * <li>dateFormatMark=null</li>
 * <li>useName=yes</li>
 * <li>regexp= ".*\.grb" (anything ending with .grb)</li>
 * </ul>
 *
 * @see "https://www.unidata.ucar.edu/projects/THREDDS/tech/tds4.2/reference/collections/CollectionSpecification.html"
 * @author caron
 * @since Jul 7, 2009
 */
@ThreadSafe
public abstract class CollectionSpecParserAbstract {
  protected final String spec;
  protected final String rootDir;
  protected final boolean subdirs; // recurse into subdirectories under the root dir
  protected final boolean filterOnName; // filter on name, else on entire path
  protected final Pattern filter; // regexp filter
  protected final String dateFormatMark;
  protected final String delimiter;
  protected final String fragment;

  /**
   * Single spec : "/topdir/** /#dateFormatMark#regExp"
   * This only allows the dateFormatMark to be in the file name, not anywhere else in the filename path,
   * and you cant use any part of the dateFormat to filter on.
   * 
   * @param collectionSpec the collection Spec
   * @param rootDir the root directory
   * @param filterAndDateMark the part of spec containing filter and/ or date mark
   * @param delimiter the delimiter in the file path
   * @param fragment in path
   * @param errlog put error messages here, may be null
   */
  protected CollectionSpecParserAbstract(String collectionSpec, String rootDir, String filterAndDateMark,
      String delimiter, String fragment, Formatter errlog) {
    this.spec = collectionSpec.trim();

    this.rootDir = rootDir;
    this.subdirs = collectionSpec.contains(delimiter + "**" + delimiter);
    this.filter = getRegEx(filterAndDateMark);
    this.dateFormatMark = getDateFormatMark(filterAndDateMark);
    this.delimiter = delimiter;
    this.fragment = fragment;
    this.filterOnName = true;
  }

  /**
   * @param rootDir the root directory
   * @param regExp the regular expression to use as a filter
   * @param delimiter the delimiter in the file path
   * @param fragment in path
   * @param errlog put error messages here, may be null
   */
  protected CollectionSpecParserAbstract(String rootDir, String regExp, String delimiter, String fragment,
      Formatter errlog) {
    this.rootDir = StringUtil2.removeFromEnd(rootDir, delimiter);
    this.subdirs = true;
    this.spec = this.rootDir + delimiter + regExp;
    this.filter = Pattern.compile(spec);
    this.dateFormatMark = null;
    this.delimiter = delimiter;
    this.fragment = fragment;
    this.filterOnName = false;
  }

  protected static String getRootDir(String collectionSpec, String defaultRootDir, String delimiter,
      boolean addTrailingDelimiter) {
    final String rootDir = splitOnLastDelimiter(collectionSpec, delimiter)[0];

    if (rootDir == null) {
      return defaultRootDir;
    }

    return addTrailingDelimiter ? rootDir + delimiter : rootDir; // TODO nicer solution then this bool? s3 needs to get
                                                                 // a trailing delimiter
  }

  protected static String getFilterAndDateMark(String collectionSpec, String delimiter) {
    return splitOnLastDelimiter(collectionSpec, delimiter)[1];
  }

  protected static String[] splitOnLastDelimiter(String collectionSpec, String delimiter) {
    if (delimiter == null || delimiter.isEmpty()) {
      return new String[] {null, collectionSpec};
    }

    final String wantSubDirs = delimiter + "**" + delimiter;
    final int startPositionOfLastDelimiter =
        collectionSpec.contains(wantSubDirs) ? collectionSpec.indexOf("/**/") : collectionSpec.lastIndexOf('/');
    final int endPositionOfLastDelimiter =
        collectionSpec.contains(wantSubDirs) ? collectionSpec.indexOf("/**/") + 3 : collectionSpec.lastIndexOf('/');

    if (startPositionOfLastDelimiter == -1) {
      return new String[] {null, collectionSpec.isEmpty() ? null : collectionSpec};
    } else if (endPositionOfLastDelimiter >= collectionSpec.length() - 1) {
      return new String[] {collectionSpec.substring(0, startPositionOfLastDelimiter), null};
    } else {
      return new String[] {collectionSpec.substring(0, startPositionOfLastDelimiter),
          collectionSpec.substring(endPositionOfLastDelimiter + 1)};
    }
  }

  protected static Pattern getRegEx(String filterAndDateMark) {
    if (filterAndDateMark == null) {
      return null;
    }

    int numberOfHashes = filterAndDateMark.length() - filterAndDateMark.replace("#", "").length();

    if (numberOfHashes == 0) {
      return Pattern.compile(filterAndDateMark);
    } else if (numberOfHashes == 1) {
      return Pattern.compile(filterAndDateMark.substring(0, filterAndDateMark.indexOf('#')) + "*");
    } else {
      StringBuilder sb = new StringBuilder(StringUtil2.remove(filterAndDateMark, '#')); // remove hashes, replace with .

      for (int i = filterAndDateMark.indexOf('#'); i < filterAndDateMark.lastIndexOf('#') - 1; i++) {
        sb.setCharAt(i, '.');
      }

      return Pattern.compile(sb.toString());
    }
  }

  protected static String getDateFormatMark(String filterAndDateMark) {
    if (filterAndDateMark == null) {
      return null;
    }

    int numberOfHashes = filterAndDateMark.length() - filterAndDateMark.replace("#", "").length();

    if (numberOfHashes == 0) {
      return null;
    } else if (numberOfHashes == 1) {
      return filterAndDateMark;
    } else {
      return filterAndDateMark.substring(0, filterAndDateMark.lastIndexOf('#'));
    }
  }

  public PathMatcher getPathMatcher() {
    if (spec.startsWith("regex:") || spec.startsWith("glob:")) { // experimental
      return FileSystems.getDefault().getPathMatcher(spec);
    } else {
      return new BySpecp();
    }
  }

  private class BySpecp implements java.nio.file.PathMatcher {
    @Override
    public boolean matches(Path path) {
      Matcher matcher = filter.matcher(path.getFileName().toString());
      return matcher.matches();
    }
  }

  public String getRootDir() {
    return rootDir;
  }

  public abstract String getFilePath(String filename);

  public String getDelimiter() {
    return delimiter;
  }

  public String getFragment() {
    return fragment;
  }

  public boolean wantSubdirs() {
    return subdirs;
  }

  public Pattern getFilter() {
    return filter;
  }

  public boolean getFilterOnName() {
    return filterOnName;
  }

  public String getDateFormatMark() {
    return dateFormatMark;
  }

  @Override
  public String toString() {
    return "CollectionSpecParser{" + "\n   topDir='" + rootDir + '\'' + "\n   subdirs=" + subdirs + "\n   regExp='"
        + filter + '\'' + "\n   dateFormatMark='" + dateFormatMark + '\'' +
        // "\n useName=" + useName +
        "\n}";
  }
}
