package aw
import play.api.libs.json._
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import java.sql.Connection
import java.sql.DriverManager
import org.postgresql.Driver
import java.sql.SQLException
import java.sql.Timestamp
import aw.terms._

object engage {


    /**Aggregate all single-item lists to one List
    * @param a list to join
    * @param b second list to join
    * @return two lists joined
    */
    def reduceDevices(a : List[Device], b: List[Device]) : List[Device] = {
        var ab = a.to[scala.collection.mutable.ArrayBuffer]
        ab += b(0)
        ab.toList
    }

    /**
    * Take only last rows for each device_ID
    * dropping duplicates in same batch, if any;
    * Like
    * {device1, time: 0}  <--- will be dropped;
    * {device1, time 11}
    * @param data - list of all devices;
    * @return reduced list
    */
    def aggregate(data : List[Device]): List[Device] = {
        var devicesData = data.sortBy( - _.timestamp)
        var devices = new ArrayBuffer[Device]
        var ab = new ArrayBuffer[Int]
        for (i <- devicesData.size - 1 to 0 by - 1){
            if(!ab.contains(devicesData(i).deviceID) && devicesData(i).statusCode == 200){
                ab += devicesData(i).deviceID
                devices += devicesData(i)
            }
        }
        devices.toList
    }

    /**
    * Calculate Haversine distance between two GPS-coordinates points
    */
    def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
        val R = 6372.8  //approx radius of Earth in km
        val dLat=(lat2 - lat1).toRadians
        val dLon=(lon2 - lon1).toRadians
        val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
        val c = 2 * asin(sqrt(a))
        R * c
    }


    // Interpretation of engageability matrix for devices and LOCATIONS;
    def engageable_with_locations(device_type: Int, loc_type: String): Boolean = {
        if (loc_type == "field"){
            val accepted = List(3, 8) // combine, add spreader, sprayer w/e
            if (accepted.contains(device_type)){ // stub for FIELD <-->Combine
                true
            }else{
                false
            }
        }else if(loc_type == "loc"){
            val accepted = List(10) // truck, add else if any;
            if (accepted.contains(device_type)){
                true
            }else{
                false
            }
        }else{
            false
        }
    }
    def estimatedTimeOfCompletion(gps_spd: Double, width: Double, fieldSize: Double):(Double, Double) = {

        //Calculate esimated time of completion based on speed and width .

        println("speed: " + gps_spd)
        val area_in_sqft: Double = (gps_spd * width)   //( This is in sq.ft)
        val area_in_acres: Double = (area_in_sqft / 43560)    //(sq.ft to acres conversion)
        val time_in_hours: Double = (fieldSize / area_in_acres)
        val time_in_sec: Double = (time_in_hours * 60 * 60)
        (time_in_sec, area_in_acres)
    }

    def insertToDB(devices: Device, etc: Double, area: Double, optime: Long, idletime: Long, c: Connection) = {
        val sql = c.prepareStatement("""INSERT INTO sensor_results("device_id","user_id","equipment_id","task_id","gps_lat","gps_long","gps_alt"
            ,"gps_spd","acc_x","acc_y","acc_z","generated_at","est_time_complete","area_acre_covered_per_hour","operating_time", "idle_time", "bearing", "timezone","created_at") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?);""")
                sql.setInt(1, devices.deviceID)
                sql.setInt(2, devices.userID)
                sql.setInt(3, devices.equipmentID)
                sql.setInt(4, devices.task.taskID)     /////////////// TASK_STATUS_CHANGE
                sql.setDouble(5, devices.geoData.gps_lat)
                sql.setDouble(6, devices.geoData.gps_long)
                sql.setDouble(7, devices.geoData.gps_alt)
                sql.setDouble(8, devices.geoData.gps_spd)
                sql.setDouble(9, devices.geoData.acc_x)
                sql.setDouble(10, devices.geoData.acc_y)
                sql.setDouble(11, devices.geoData.acc_z)
                sql.setTimestamp(12, new Timestamp(devices.timestamp))
                sql.setDouble(13, etc)
                sql.setDouble(14, area)
                sql.setDouble(15, optime/1000000000)
                sql.setDouble(16, idletime/1000)
                sql.setDouble(17,if (devices.geoData.bearing.length == 0) 0 else devices.geoData.bearing.toDouble)
                sql.setString(18, "UTC")
                sql.setTimestamp(19,  new Timestamp(System.currentTimeMillis()))
                try
                {
                    sql.executeUpdate()
                }
                catch
                {
                    case e: SQLException => println("Error while Inserting into sensor_result: "+e.printStackTrace)
                    case _: Throwable => println("Got some other kind of exception")
                }
                //finally
                //{
                //c.close()
            //}
        }


        def getWidthField(id: Int, access: List[String]) : Int = {
            // Query the database using device_id to get the fieldsize and width of equipment
            val driver = "org.postgresql.Driver"
            val url = "jdbc:postgresql://" + access(0)
            val user = access(1)
            val password = access(2)
            Class.forName(driver)
            val c = DriverManager.getConnection(url,user,password)
            val sql = c.prepareStatement("""select width from tasks where user_id = ?""")  //Add the SQL to fetch width and field info
            sql.setInt(1, id)
            var result = sql.executeQuery()
            if(result.next()){
                c.close()
                result.getInt("width")
            }else{
                c.close()
                -1
            }
        }

        def getTaskEquipment(id: Int, c: Connection): Int= {
            // Query the database using device_id to get the fieldsize and width of equipment
            val sql = c.prepareStatement("""select task_id from view_user_tasks where device_id = ?""")
            sql.setInt(1, id)
            var task_id: Int = 0
            try{
                var result = sql.executeQuery()
                if(result.next())
                task_id = result.getInt("task_id")
            }catch{
                case e: Exception => println("Error getting task_id and equipment_id: "+e.printStackTrace)
            }
            task_id
        }


}
