import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.opencsv.CSVReader
import java.io.FileReader
import java.io.File
import scala.xml.XML
import java.sql.{DriverManager, SQLException}

object Util {
    def xmlParse(file: String): Map[String, String] = {
      val xml = XML.loadFile(file)
      Map(
      "coreXml" -> s"${xml \ "coreXml" text}",
      "hdfsXml" -> s"${xml \ "hdfsXml" text}",
      "hiveXml" -> s"${xml \ "hiveXml" text}",
      "userKeytab" -> s"${xml \ "userKeytab" text}",
      "src" -> s"${xml \ "src" text}",
      "dest" -> s"${xml \ "dest" text}",
      "tableType" -> s"${xml \ "tableType" text}",
      "isHeader" -> s"${xml \ "isHeaders" text}",
      "delimiter" -> s"${xml \ "delimiter" text}",
      "quoteChar" -> s"${xml \ "quoteChar" text}",
      "escapeChar" -> s"${xml \ "escapeChar" text}",
      "dbTable" -> s"${xml \ "dbTable" text}",
      "thriftServer" -> s"${xml \ "thriftServer" text}"
      )
    }

    def hadoopConf(coreXml: String, hdfsXml: String, hiveXml: String, userKeytab: String): Configuration = {
      val conf = new Configuration
      conf.addResource(new Path(coreXml))
      conf.addResource(new Path(hdfsXml))
      conf.addResource(new Path(hiveXml))
      conf.set("hadoop.job.ugi", System.getProperty("user.name"))
      UserGroupInformation.setConfiguration(conf)
      if (conf.get("hadoop.security.authentication") == "kerberos")
        UserGroupInformation.loginUserFromKeytab(System.getProperty("user.name"), userKeytab)
      
      conf
    }

    def hadoopFS(conf: Configuration): FileSystem = FileSystem.get(conf)
}

class TextToHive(xml: Map[String, String]) {
  val config = xml
  val hConf = Util.hadoopConf(config("coreXml"), config("hdfsXml"), config("hiveXml"), config("userKeytab"))
  val hFs = Util.hadoopFS(hConf)

  private def createCols(): List[String] = {

    val srcFolder = {
        val f = new File(config("src"))
        if (f.exists() && f.isDirectory())
          f
        else
          throw new Exception("Please put a folder path that exists in the xml.")
        }

    val textFile = srcFolder.listFiles.toList(0)
    val reader = new CSVReader(new FileReader(textFile), config("delimiter")(0))
    val head = reader.readNext().toList
    if (config("isHeader") == "yes") {
      def cleanEnding(s: String): String = if (s.endsWith("_")) cleanEnding(s.dropRight(1)) else s
      head.map(x => cleanEnding(x.replaceAll("[^a-zA-Z0-9]+","_").toLowerCase()))
      }
    else
      List.tabulate(head.length)(n => "col" + (n + 1).toString)
  }

  private def buildSql(): String = {
    val sqlStatement = new StringBuilder

    val crteTable = {
        if (config("tableType") == "external")
          f"Create ${config("tableType")} table ${config("dbTable")}" + " (\n"
        else
          f"Create table ${config("dbTable")}" + " (\n"
    }

    val rowFormat = s"""ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      |WITH SERDEPROPERTIES (
      |  'separatorChar' = '\\${config("delimiter")}',
      |  'quoteChar' = '\\${config("quoteChar")}',
      |  'escapeChar' = '\\${config("escapeChar")}'
      |  )
      |STORED AS TEXTFILE \n""".stripMargin

    val location = {
        //val dest = conf("dest"))
        f"LOCATION '${config("dest")}'\n"
      }

    val tblpropHeader = """TBLPROPERTIES ('skip.header.line.count'='1')"""
    val cols = createCols()

    //Put staetment together
    sqlStatement.append(crteTable)
    for (col <- cols) {
      if (col != cols.last)
        sqlStatement.append(f" $col string, \n")
      else
        sqlStatement.append(f" $col string) \n")
    }

    sqlStatement.append(rowFormat)
    if (config("tableType") == "external")
      sqlStatement.append(location)
    if (config("isHeader") == "yes")
      sqlStatement.append(tblpropHeader)
    
    println("Created HQL DDL Query!")
    print(sqlStatement.toString)
    sqlStatement.toString
  }
  private def mvFiles(): Unit = {
    val srcPath = new Path(config("src"))
    val hiveWarehouse = hConf.get("hive.metastore.warehouse.dir")
    val table = config("dbTable").split('.')(1)
    val db = config("dbTable").split('.')(0)
    val dest: Path = {
        if (config("tableType") == "internal")

          new Path(hiveWarehouse + s"/$db.db/$table")
        else
          new Path(config("dest"))
      }
    def hadoopCheckDir(path: Path): Path = {
        if (hFs.exists(path))
        println("Folder exists in HDFS deleting...")
        hFs.delete(path)
        path
      }
    println(dest.toString)

    hFs.copyFromLocalFile(srcPath, hadoopCheckDir(dest))
    println(s"Moved files to HDFS Folder ${dest}!")
  }

  @throws(classOf[SQLException])
  def hiveTable(): Unit = {
    val hql = buildSql()
    val principal = hConf.get("hive.metastore.kerberos.principal")
    val cnxnString = new StringBuilder
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    
    cnxnString.append(s"jdbc:hive2://${config("thriftServer")}/;")
    if (hConf.get("hadoop.security.authentication") == "kerberos")
      cnxnString.append(s"principal=${principal}")
    val cnxn = DriverManager.getConnection(cnxnString.toString)
    val stmt = cnxn.createStatement()
      //try {
    mvFiles()
    stmt.execute(hql)
    println("Created Hive Table!")
  }
}

object Launch {
  def main(args: Array[String]) {
    for (arg <- args) {
      val xml = Util.xmlParse(arg)
      val qry = new TextToHive(xml)
        qry.hiveTable()
    }
  }
}