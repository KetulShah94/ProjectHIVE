package com.mcit.winter2020.hadoop.hive
import java.sql.{Connection, DriverManager}

class HiveClient {
  val driverName:String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.
    getConnection("jdbc:hive2://172.16.129.58:10000/winter2020_ketul;user=ketul;password=ketul")
}
