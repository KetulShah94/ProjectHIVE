package com.mcit.winter2020.hadoop.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object EnrichTrip extends HiveClient with App {

  println("EnrichedTrip Tables!")
  val stmt = connection.createStatement()
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("SET hive.exec.dynamic.partition = true")

  /*val conf = new Configuration()
  var localPath: String = "/home/ketul/opt/hadoop-2.7.7/etc/cloudera/"
  conf.addResource(new Path(localPath + "core-site.xml"))
  conf.addResource(new Path(localPath + "hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)*/


  /*Deleting dir*/
//  if (fs.exists(new Path("/user/winter2020/ketul/project4"))) {
//    println("deleting file: /user/winter2020/ketul/project4")
//    fs.delete(new Path("/user/winter2020/ketul/project4"), true)
//  } else
//    println("Does not exists")
//
//  /*creating dir*/
//  fs.mkdirs(new Path("/user/winter2020/ketul/project4"))
//  println("Created sub folder project4")

  /* drop and create of tables*/
  stmt.execute(" DROP TABLE IF EXISTS winter2020_ketul.ext_trips")
  stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "  + " winter2020_ketul.ext_trips ( " +
    " route_id SMALLINT," +
    " service_id STRING, " +
    " trip_id STRING," +
    " trip_headsign STRING," +
    " direction_id TINYINT," +
    " shape_id INT," +
    " wheelchair_accessible TINYINT," +
    " note_fr STRING," +
    " note_en STRING)" +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE " +
    " LOCATION '/user/winter2020/ketul/project4/trips' " +
    " tblproperties ('skip.header.line.count' = '1', 'serialization.null.format' = '')")
  println("ext_trips is created")

  stmt.execute(" DROP TABLE IF EXISTS winter2020_ketul.ext_calendar_dates")
  stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "  + "winter2020_ketul.ext_calendar_dates ( " +
    " service_id STRING, " +
    " date STRING, " +
    " exception_type TINYINT) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE " +
    " LOCATION '/user/winter2020/ketul/project4/calendar_dates' " +
    " tblproperties ('skip.header.line.count' = '1', 'serialization.null.format' = '')")
  println("ext_calendar_dates is created")

  stmt.execute(" DROP TABLE IF EXISTS winter2020_ketul.ext_frequencies")
  stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "  + "winter2020_ketul.ext_frequencies ( " +
    " trip_id STRING," +
    " start_time STRING, " +
    " end_time STRING, " +
    " headway_secs SMALLINT) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE " +
    " LOCATION '/user/winter2020/ketul/project4/frequencies' " +
    " tblproperties ('skip.header.line.count' = '1', 'serialization.null.format' = '')")
  println("ext_frequencies is created")
//
//  /*CopyFromHDFSToHDFS*/
//  val source ="/user/winter2020/ketul/stm/"
//  val destination ="/user/winter2020/ketul/project4/"
//
//  org.apache.hadoop.fs.FileUtil.copy(
//    new org.apache.hadoop.fs.Path(source + "trips.txt").getFileSystem(conf),
//    new org.apache.hadoop.fs.Path(source + "trips.txt"),
//      new Path(destination + "tirps").getFileSystem(conf),
//    new Path(destination + "tirps"),
//    true,
//    conf
//  )
//  println("Trips file copy from hdfs")

//  org.apache.hadoop.fs.FileUtil.copy(
//    new Path(source + "routes.txt").getFileSystem(conf),
//    new Path(source + "routes.txt"),
//      new Path(destination + "frequencies").getFileSystem(conf),
//    new Path(destination + "frequencies"),
//    true,
//      conf)
//  println("Routes file copy from hdfs")
//
//  org.apache.hadoop.fs.FileUtil.copy(
//    new Path(source + "calender.txt").getFileSystem(conf),
//    new Path(source + "calender.txt"),
//    new Path(destination + "calendar_date").getFileSystem(conf),
//    new Path(destination + "calendar_date"),
//      true,
//  conf)
//  println("Calender file copy from hdfs")


  val tableName: String = "enrichedtrip"
  stmt.execute("DROP TABLE IF EXISTS " + tableName)

  stmt.execute("CREATE TABLE winter2020_ketul.enrichedtrip ( " +
    "route_id INT, " +
    "service_id STRING," +
    "trip_id STRING," +
    "trip_headsign STRING," +
    "direction_id INT," +
    "shape_id INT," +
    "note_fr STRING," +
    "note_en STRING," +
    "date STRING," +
    "exception_type INT," +
    "start_time STRING," +
    "end_time STRING," +
    "headway_secs INT )" +
    "PARTITIONED BY(wheelchair_accessible INT)  " +
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  " +
    " STORED AS PARQUET ")

  stmt.execute("INSERT OVERWRITE TABLE winter2020_ketul.enrichedtrip PARTITION (wheelchair_accessible) " +
    "SELECT ext_trips.route_id , " +
    "ext_trips.service_id ," +
    "ext_trips.trip_id," +
    "ext_trips.trip_headsign," +
    "ext_trips.direction_id ," +
    "ext_trips.shape_id ," +
    "ext_trips.note_fr," +
    "ext_trips.note_en," +
    "ext_calendar_dates.date," +
    "ext_calendar_dates.exception_type, " +
    "ext_frequencies.start_time, " +
    "ext_frequencies.end_time," +
    "ext_frequencies.headway_secs," +
    "ext_trips.wheelchair_accessible  " +
    "FROM winter2020_ketul.ext_trips " +
    "LEFT JOIN winter2020_ketul.ext_calendar_dates ON ext_trips.service_id=ext_calendar_dates.service_id " +
    "LEFT JOIN ext_frequencies  ON ext_trips.trip_id=ext_frequencies.trip_id" )

  println("enrichedtable created sucessfully ")

  stmt.close()
  connection.close()

}