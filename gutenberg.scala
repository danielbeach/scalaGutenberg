import java.io.{File, BufferedOutputStream, FileInputStream, FileOutputStream}
import com.github.tototoshi.csv._
import org.apache.commons.net.ftp.{FTPClient, FTPClientConfig, FTPFile}


class GutenbergFTP(host_name: String) {
  val ftp = new FTPClient
  val config = new FTPClientConfig

  def setup_ftp()= {
    ftp.configure(config)
    ftp.connect(host_name)
    ftp.enterLocalPassiveMode()
    ftp.login("anonymous", "")
    val reply = ftp.getReplyCode
    println(s"Reply code was $reply")
  }

  def list_ftp_files(): Array[FTPFile] = {
    ftp.listFiles(".").filter(FTPFile => FTPFile.isFile).filter(FTPFile => FTPFile.getName.contains(".txt"))
  }

  def write_ftp_file(file: FTPFile)= {
    val output_file = new File(file.getName)
    val out_stream = new BufferedOutputStream(new FileOutputStream(output_file))
    ftp.retrieveFile(file.getName, out_stream)
    out_stream.close()
  }
}

class sGutenberg extends GutenbergFTP(host_name = "aleph.gutenberg.org") {
  val input_csv: String = "input.csv"

  def csv_iterator(): Iterator[Seq[String]] = {
    val reader = CSVReader.open(input_csv)
    val csv_rows = reader.iterator
    csv_rows.next() //get past header
    csv_rows
  }

  def get_file_location(file_number: String): String = {
    """Files are structured into directories by splitting each number, up UNTIL the last number. Then a folder
        named with the file number. So if a file number is 418, it is located at 4/1/418. Below 10 is just 0/filenumber."""
    val folder_numbers: String = file_number.slice(0,file_number.length-1).toList.mkString("/")
    val ftp_directory_location: String = s"$folder_numbers/$file_number"
    ftp_directory_location
  }

}

object gutenberg {
  def main(args: Array[String]): Unit = {
    val sG = new sGutenberg
    sG.setup_ftp()
    val csv_rows = sG.csv_iterator()
    for (row <- csv_rows) {
       val file_number: String = row(1)
       val remote_location: String = sG.get_file_location(file_number)
       println(s"Working on directory $remote_location")
       sG.ftp.changeWorkingDirectory(remote_location)
       val files = sG.list_ftp_files()
       for (f <- files){
         val file_name: String = f.getName
         println(s"Downloading file $file_name")
         sG.write_ftp_file(f)
       }
      sG.ftp.changeToParentDirectory()
      }
    }
  }
