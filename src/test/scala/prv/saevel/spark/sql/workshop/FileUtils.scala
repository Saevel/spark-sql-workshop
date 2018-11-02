package prv.saevel.spark.sql.workshop

import java.nio.file.{Files, Path, Paths}

trait FileUtils {

  protected def withDirectoryRemoved[T](path: String)(f: => T): T = {
    deleteDirectoryIfExists(path)
    f
  }

  protected def deleteFileIfExists(path: String): Boolean =
    Files.deleteIfExists(Paths.get(System.getProperty("user.dir")).resolve(path))

  protected def deleteDirectoryIfExists(path: String): Boolean = {
    val directoryPath: Path = Paths.get(System.getProperty("user.dir")).resolve(path)

    val files = directoryPath.toFile.listFiles
    if(files != null) {
      directoryPath.toFile.listFiles.foreach(file =>
        if(file isDirectory){
          deleteDirectoryIfExists(file.getPath)
        } else {
          deleteFileIfExists(file.getPath)
        }
      )
    }

    Files.deleteIfExists(directoryPath)
  }
}
