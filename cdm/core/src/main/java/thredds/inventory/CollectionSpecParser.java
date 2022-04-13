package thredds.inventory;

import java.io.File;
import java.util.Formatter;

public class CollectionSpecParser extends CollectionSpecParserAbstract {
  private final static String DELIMITER = "/";
  private final static String FRAGMENT = "";
  private final static String DEFAULT_DIR = System.getProperty("user.dir");

  /**
   * Single spec : "/topdir/** /#dateFormatMark#regExp"
   * This only allows the dateFormatMark to be in the file name, not anywhere else in the filename path,
   * and you cant use any part of the dateFormat to filter on.
   *
   * @param collectionSpec the collection Spec
   * @param errlog put error messages here, may be null
   */
  public CollectionSpecParser(String collectionSpec, Formatter errlog) {
    super(collectionSpec, getRootDir(collectionSpec, DEFAULT_DIR, DELIMITER, false),
        getFilterAndDateMark(collectionSpec, DELIMITER), DELIMITER, FRAGMENT, errlog);

    File locFile = new File(rootDir);
    if (!locFile.exists() && errlog != null) {
      errlog.format(" Directory %s does not exist %n", rootDir);
    }
  }

  /**
   * @param rootDir the root directory
   * @param regExp the regular expression to use as a filter
   * @param errlog put error messages here, may be null
   */
  public CollectionSpecParser(String rootDir, String regExp, Formatter errlog) {
    super(rootDir, regExp, DELIMITER, FRAGMENT, errlog);
  }

  @Override
  public String getFilePath(String filename) {
    return rootDir.endsWith(delimiter) ? rootDir + filename : rootDir + delimiter + filename;
  }
}
