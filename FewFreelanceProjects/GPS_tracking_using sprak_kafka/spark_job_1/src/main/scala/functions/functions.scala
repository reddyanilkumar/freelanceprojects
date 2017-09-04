package aw
import scala.collection.mutable.Map
import scala.math.max
import java.sql.Connection
import aw.terms._
import play.api.libs.json.Json



object functions{
    def makePoint(long: Double, lat:Double): String = {
        s"point($long $lat)"
    }

    def parse(jstr: String) : List[Device] = {
        val res = Json.parse(jstr)
        val device = Device(
            deviceID = (res \ "device_id").as[String].toInt,
            userID = (res \ "user_id").as[String].toInt,
            timestamp = ((res \ "timestamp").as[String] +"000").toLong, // NIFI sends SECONDS, we need MILLISeconds
            statusCode = (res \ "status_code").as[String].toInt,
            equipmentID = (res \ "equipment_id").as[String].toInt,

            geoData = GPSProperties(
                (res \ "gps_lat").as[String].toDouble,
                (res \ "gps_long").as[String].toDouble,
                (res \ "gps_alt").as[String].toDouble,
                (res \ "gps_spd").as[String].toDouble,
                (res \ "acc_x").as[String].toDouble,
                (res \ "acc_y").as[String].toDouble,
                (res \ "acc_z").as[String].toDouble,
                (res \ "bearing").as[String]
            ),
            equipmentType = 0,
            task = Task(0, "", 0, 0, 0, 0, 0, "", ""), // will be queried from DB
            locationID = -1,
            inActivity = false, // it is not in any activity. just yet;
            width = 0,
            isInBoundary = false
        )
        List(device) // returns a single-item list;
    }
    def updateTask(key: (String, String, Int, Set[Int])): String = {
        val patterns = Map[(String, String, Int, Set[Int]), String](

            ("Harvesting", "Load Grains to Trucks", EVENT.ENGAGE, Set(1, 3)) -> STATUS.LOADING,
            ("Harvesting", "Load Grains to Trucks", EVENT.DISENGAGE, Set(1, 3)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Trucks", EVENT.ENGAGE, Set(1, 10)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Trucks", EVENT.DISENGAGE, Set(1, 10)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Trucks", EVENT.ENTER_BOUNDARY, Set(1, -3)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Trucks", EVENT.LEAVE_LOC100, Set(1, -3)) -> STATUS.COMPLETED,
            ("Harvesting", "Load Grains to Trucks", EVENT.IDLE, Set(1, -3)) -> STATUS.IDLE, // idle on field
            ("Harvesting", "Load Grains to Trucks", EVENT.NOT_IDLE, Set(1,-3)) -> STATUS.DRIVING,

            ("Harvesting", "Harvesting", EVENT.ENGAGE, Set(3, 1) ) -> STATUS.UNLOADING,
            ("Harvesting", "Harvesting", EVENT.ENTER_BOUNDARY, Set(3,-3)) -> STATUS.OPERATING,
            ("Harvesting", "Harvesting", EVENT.DISENGAGE, Set(3, 1)) -> STATUS.OPERATING,
            ("Harvesting", "Harvesting", EVENT.ONLOC, Set(3, -3)) -> STATUS.OPERATING,
            ("Harvesting", "Harvesting", EVENT.LEAVE_LOC100, Set(3, -3)) -> STATUS.COMPLETED,
            ("Harvesting", "Harvesting", EVENT.IDLE, Set(3, -3)) -> STATUS.IDLE,
            ("Harvesting", "Harvesting", EVENT.NOT_IDLE, Set(3, -3)) -> STATUS.OPERATING,

            ("Harvesting", "Load Grains to Bin", EVENT.ENGAGE, Set(10, 1)) -> STATUS.LOADING,
            ("Harvesting", "Load Grains to Bin", EVENT.DISENGAGE, Set(10,1)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Bin", EVENT.LEAVE_LOC100, Set(10, -2)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Bin", EVENT.LEAVE_LOC100, Set(10, -1)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Bin", EVENT.ENTER_BOUNDARY, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Bin", EVENT.ONLOC, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Bin", EVENT.IDLE, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Bin", EVENT.ENTER_BOUNDARY, Set(10, 0)) -> STATUS.COMPLETED,
            ("Harvesting", "Load Grains to Bin", EVENT.IDLE, Set(10, -1)) -> STATUS.IDLE,
            ("Harvesting", "Load Grains to Bin", EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            ("Harvesting", "Load Grains to Bin", EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,

            ("Harvesting", "Load Grains to Customers", EVENT.ENTER_BOUNDARY, Set(10,-1)) -> STATUS.LOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Customers", EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            ("Harvesting", "Load Grains to Customers", EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,
            ("Harvesting", "Load Grains to Customers", EVENT.ENTER_BOUNDARY, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.ONLOC, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.IDLE, Set(10, -2)) -> STATUS.UNLOADING,
            ("Harvesting", "Load Grains to Customers", EVENT.ENTER_BOUNDARY, Set(10, 0)) -> STATUS.COMPLETED

        )
        patterns.get(key).getOrElse(STATUS.UNDEFINED)
    }
    /*
    def updateTask(key: (Int, Int, Int, Set[Int]) ): String = {
        val patterns = Map[(Int, Int, Int, Set[Int]), String](
            // 9, 1 - Harvesting, Transfer grains
            (9, 1, EVENT.ENGAGE, Set(1, 3)) -> STATUS.LOADING,
            (9, 1, EVENT.DISENGAGE, Set(1, 3)) -> STATUS.DRIVING,
            (9, 1, EVENT.ENGAGE, Set(1, 10)) -> STATUS.UNLOADING,
            (9, 1, EVENT.DISENGAGE, Set(1, 10)) -> STATUS.DRIVING,
            (9, 1, EVENT.ENTER_BOUNDARY, Set(1, -3)) -> STATUS.DRIVING,
            (9, 1, EVENT.LEAVE_LOC100, Set(1, -3)) -> STATUS.COMPLETED,
            (9, 1, EVENT.IDLE, Set(1, -3)) -> STATUS.IDLE, // idle on field
            (9, 1, EVENT.NOT_IDLE, Set(1,-3)) -> STATUS.DRIVING,
            // part of transitioning
            (9, 1, EVENT.IDLE, Set(1, -4)) -> STATUS.IDLE, // idle somewhere
            (9, 1, EVENT.NOT_IDLE, Set(1, -4)) -> STATUS.DRIVING,
            // 9, 2 - Harvesting, Harvesting
            (9, 2, EVENT.ENGAGE, Set(3, 1) ) -> STATUS.UNLOADING,
            (9, 2, EVENT.ENTER_BOUNDARY, Set(3,-3)) -> STATUS.OPERATING,
            (9, 2, EVENT.DISENGAGE, Set(3, 1)) -> STATUS.OPERATING,
            (9, 2, EVENT.ONLOC, Set(3, -3)) -> STATUS.OPERATING,
            (9, 2, EVENT.LEAVE_LOC100, Set(3, -3)) -> STATUS.COMPLETED,
            (9, 2, EVENT.IDLE, Set(3, -3)) -> STATUS.IDLE,
            (9, 2, EVENT.NOT_IDLE, Set(3, -3)) -> STATUS.OPERATING,
            // 9, 3 - Harvesting, Transfer Loads
            (9, 3, EVENT.ENGAGE, Set(10, 1)) -> STATUS.LOADING,
            (9, 3, EVENT.DISENGAGE, Set(10,1)) -> STATUS.DRIVING,
            (9, 3, EVENT.LEAVE_LOC100, Set(10, -2)) -> STATUS.DRIVING,
            (9, 3, EVENT.LEAVE_LOC100, Set(10, -1)) -> STATUS.DRIVING,
            (9, 3, EVENT.ENTER_BOUNDARY, Set(10, -2)) -> STATUS.UNLOADING,
            (9, 3, EVENT.ONLOC, Set(10, -2)) -> STATUS.UNLOADING,
            (9, 3, EVENT.IDLE, Set(10, -2)) -> STATUS.UNLOADING,
            (9, 3, EVENT.ENTER_BOUNDARY, Set(10, 0)) -> STATUS.COMPLETED,
            (9, 3, EVENT.IDLE, Set(10, -1)) -> STATUS.IDLE,
            (9, 3, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            (9, 3, EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,

            // 8, 1 - chemical spray, 1 Haul chemical sprayer
            (8, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (8, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (8, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (8, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (8, 1, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (8, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            (8, 1, EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,
            // 8, 2 - chemical spray, unloading
            (8, 2, EVENT.ENGAGE, Set(10, 8)) -> STATUS.UNLOADING,
            (8, 2, EVENT.LEAVE_LOC100, Set(10, -3)) -> STATUS.COMPLETED,
            (8, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            // 8, 3 - chemical spray, spraying
            (8, 3, EVENT.ENGAGE, Set(10,8)) -> STATUS.LOADING,
            (8, 3, EVENT.IDLE, Set(8, -3)) -> STATUS.IDLE,
            (8, 3, EVENT.ONLOC, Set(8, -3)) -> STATUS.OPERATING,
            (8, 3, EVENT.LEAVE_LOC100, Set(8, -3)) -> STATUS.COMPLETED,

            // 7, 1 - SDB, 1 Haul chemical sprayer
            (7, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (7, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (7, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (7, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (7, 1, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (7, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            (7, 1, EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,
            // 7, 2 - SDB, unloading
            (7, 2, EVENT.ENGAGE, Set(10, 7)) -> STATUS.UNLOADING,
            (7, 2, EVENT.LEAVE_LOC100, Set(10, -3)) -> STATUS.COMPLETED,
            (7, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            // 7, 3 - SDB sidedressing
            (7, 3, EVENT.ENGAGE, Set(10, 7)) -> STATUS.LOADING,
            (7, 3, EVENT.IDLE, Set(7, -3)) -> STATUS.IDLE,
            (7, 3, EVENT.ONLOC, Set(7, -3)) -> STATUS.OPERATING,
            (7, 3, EVENT.LEAVE_LOC100, Set(7, -3)) -> STATUS.COMPLETED,

            // Plantong 6 hauling seeds 1
            (6, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (6, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (6, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (6, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (6, 1, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (6, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            (6, 1, EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,
            // 6, 2 - Planting, unloading Seed
            (6, 2, EVENT.ENGAGE, Set(10, 6)) -> STATUS.UNLOADING,
            (6, 2, EVENT.LEAVE_LOC100, Set(10, -3)) -> STATUS.COMPLETED,
            (6, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            // 6, 3 - Planting, hauling starter
            (6, 3, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (6, 3, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (6, 3, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (6, 3, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (6, 3, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (6, 3, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,
            (6, 3, EVENT.NOT_IDLE, Set(10, -4)) -> STATUS.DRIVING,
            // 6, 4 - Planting, unloading starter
            (6, 4, EVENT.ENGAGE, Set(10, 6)) -> STATUS.UNLOADING,
            (6, 4, EVENT.LEAVE_LOC100, Set(10, -3)) -> STATUS.COMPLETED,
            (6, 4, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            // 6, 5 planting, Planting
            (6, 3, EVENT.ENGAGE, Set(10, 7)) -> STATUS.LOADING,
            (6, 3, EVENT.IDLE, Set(7, -3)) -> STATUS.IDLE,
            (6, 3, EVENT.ONLOC, Set(7, -3)) -> STATUS.OPERATING,
            (6, 3, EVENT.LEAVE_LOC100, Set(7, -3)) -> STATUS.COMPLETED,

            //5, 1 Cultivation, Cultivation
            (5, 1, EVENT.ENTER_BOUNDARY, Set(4, -3)) -> STATUS.OPERATING,
            (5, 1, EVENT.ONLOC, Set(4, -3)) -> STATUS.OPERATING,
            (5, 1, EVENT.IDLE, Set(4, -3)) -> STATUS.IDLE,
            (5, 1, EVENT.LEAVE_LOC100, Set(4, -3)) -> STATUS.COMPLETED,

            // 4, 1 Liquid Fertilizer, hauling
            (4, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (4, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (4, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (4, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (4, 1, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (4, 1, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (4, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,

            // 4, 2, UNLOADING
            (4, 2, EVENT.ENGAGE, Set(10, 8)) -> STATUS.UNLOADING,
            (4, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (4, 2, EVENT.LEAVE_LOC100, Set(10,-3)) -> STATUS.COMPLETED,

            // 4, 3 spraying
            (4, 3, EVENT.ENGAGE, Set(10, 8)) -> STATUS.LOADING,
            (4, 3, EVENT.IDLE, Set(8, -3)) -> STATUS.IDLE,
            (4, 3, EVENT.ONLOC, Set(8, -3)) -> STATUS.OPERATING,
            (4, 3, EVENT.LEAVE_LOC100, Set(8, -3)) -> STATUS.COMPLETED,

            // Chisel, Chisel plow
            (3, 1, EVENT.ONLOC, Set(2, -3)) -> STATUS.OPERATING,
            (3, 1, EVENT.IDLE, Set(2, -3)) -> STATUS.IDLE,
            (3, 1, EVENT.LEAVE_LOC100, Set(2,-3)) -> STATUS.COMPLETED,

            // 2, 1 Lime, Hauling
            (2, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (2, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (2, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (2, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (2, 1, EVENT.ONLOC, Set(10, -2)) -> STATUS.UNLOADING,
            (2, 1, EVENT.LEAVE_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (2, 1, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (2, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,

            // Lime Unloading
            (2, 2, EVENT.ENGAGE, Set(10, 9)) -> STATUS.UNLOADING,
            (2, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (2, 2, EVENT.LEAVE_LOC100, Set(10,-3)) -> STATUS.COMPLETED,

            // 2, 3 Spreading
            (2, 3, EVENT.ENGAGE, Set(10, 9)) -> STATUS.LOADING,
            (2, 3, EVENT.IDLE, Set(9, -3)) -> STATUS.IDLE,
            (2, 3, EVENT.ONLOC, Set(9, -3)) -> STATUS.OPERATING,
            (2, 3, EVENT.LEAVE_LOC100, Set(9, -3)) -> STATUS.COMPLETED,


            // 1, 1 DryFertilizer, Hauling
            (1, 1, EVENT.ENTER_BOUNDARY, Set(10, -1)) -> STATUS.LOADING,
            (1, 1, EVENT.IDLE, Set(10, -1)) -> STATUS.LOADING,
            (1, 1, EVENT.ONLOC, Set(10, -1)) -> STATUS.LOADING,
            (1, 1, EVENT.LEAVE_BOUNDARY, Set(10, -1)) -> STATUS.DRIVING,
            (1, 1, EVENT.ENTER_LOC100, Set(10, -2)) -> STATUS.COMPLETED,
            (1, 1, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (1, 1, EVENT.IDLE, Set(10, -4)) -> STATUS.IDLE,

            // 1, 2 DF Unloading
            (1, 2, EVENT.ENGAGE, Set(10, 9)) -> STATUS.UNLOADING,
            (1, 2, EVENT.IDLE, Set(10, -3)) -> STATUS.IDLE,
            (1, 2, EVENT.LEAVE_LOC100, Set(10,-3)) -> STATUS.COMPLETED,

            // 1, 3 DF Spreading
            (1, 3, EVENT.ENGAGE, Set(10, 9)) -> STATUS.LOADING,
            (1, 3, EVENT.IDLE, Set(9, -3)) -> STATUS.IDLE,
            (1, 3, EVENT.ONLOC, Set(9, -3)) -> STATUS.OPERATING,
            (1, 3, EVENT.LEAVE_LOC100, Set(9, -3)) -> STATUS.COMPLETED
        )
        patterns.get(key).getOrElse(STATUS.UNDEFINED)
    }*/
    def isEndOfCycle(processName: String, activityName: String, currentStatus: String, newStatus: String): Boolean = {
        val entry = (processName, activityName, currentStatus, newStatus)
        entry match {
            case ("Harvesting", "Load Grains to Trucks", STATUS.UNLOADING, _) => true
            case ("Harvesting", "Load Grains to Trucks", _, STATUS.COMPLETED) => true
            case ("Harvesting", "Harvesting", _, STATUS.COMPLETED) => true
            case ("Harvesting", "Transfer Loads", STATUS.UNLOADING, _) => true
            case ("Harvesting", "Transfer Loads", _, STATUS.COMPLETED) => true
            case _ => false
        }

    }

    /*
    def isEndOfCycle (processID:Int, activityID: Int, currentStatus: String, newStatus: String): Boolean = {
        val entry = (processID, activityID, currentStatus, newStatus)
        entry match {
           case (9, 1, STATUS.UNLOADING, _) => true
           case (9, 1, _, STATUS.COMPLETED) => true
           case (9, 2, _, STATUS.COMPLETED) => true
           case (9, 3, STATUS.UNLOADING, _) => true
           case (9, 3, _, STATUS.COMPLETED) => true
           case (8, 1, _, STATUS.COMPLETED) => true
           case (8, 2, STATUS.UNLOADING, _) => true
           case (8, 3, _, STATUS.LOADING) => true
           case (7, 1, _, STATUS.COMPLETED) => true
           case (7, 2, STATUS.UNLOADING, _) => true
           case (7, 3, _, STATUS.LOADING) => true
           case (6, 1, _, STATUS.COMPLETED) => true
           case (6, 2, STATUS.UNLOADING, _) => true
           case (6, 3, _, STATUS.COMPLETED) => true
           case (6, 4, STATUS.UNLOADING, _) => true
           case (6, 5, _, STATUS.LOADING) => true
           case (5, 5, _, STATUS.COMPLETED) => true
           case (4, 1, _, STATUS.COMPLETED) => true
           case (4, 2, STATUS.UNLOADING, _) => true
           case (4, 3, _, STATUS.LOADING) => true
           case (3, 1, _, STATUS.COMPLETED) => true
           case (2, 1, _, STATUS.COMPLETED) => true
           case (2, 2, STATUS.UNLOADING, _) => true
           case (2, 3, _, STATUS.LOADING) => true
           case (1, 1, _, STATUS.COMPLETED) => true
           case (1, 2, STATUS.UNLOADING, _) => true
           case (1, 3, _, STATUS.LOADING) => true
           case _ => false
       }
   }*/
   def isEndOfTask(processName :String, activityName: String, newStatus: String): Boolean = {
       val entry = (processName, activityName, newStatus)
       entry match {
           case (_, _, STATUS.COMPLETED) => true
           case _ => false
       }
   }
   /*
   // is this function really necessary?
   // case (_, _, STATUS.COMPLETED) => true ????
   def isEndOfTask(procesID: Int, activityID: Int, newStatus: String): Boolean = {
       val entry = (processID, activityID, newStatus)
       entry match{
            case (9, 1, STATUS.COMPLETED) => true
            case (9, 2, STATUS.COMPLETED) => true
            case (9, 3, STATUS.COMPLETED) => true
            case (8, 1, STATUS.COMPLETED) => true
            case (8, 2, STATUS.COMPLETED) => true
            case (8, 3, STATUS.COMPLETED) => true
            case (7, 1, STATUS.COMPLETED) => true
            case (7, 2, STATUS.COMPLETED) => true
            case (7, 3, STATUS.COMPLETED) => true
            case (6, 1, STATUS.COMPLETED) => true
            case (6, 2, STATUS.COMPLETED) => true
            case (6, 3, STATUS.COMPLETED) => true
            case (6, 4, STATUS.COMPLETED) => true
            case (6, 5, STATUS.COMPLETED) => true
            case (5, 1, STATUS.COMPLETED) => true

            case (_, _, STATUS.COMPLETED) => true

            case _ => false
        }
    }*/


    def updateDB(c: Connection, device: Device, status: String, increaseCycle: Boolean, taskEnd: Boolean) {

        val taskID = device.task.taskID
        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // UPDATE NEXT LINE FOR SERVER DB!
        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        var sql = c.prepareStatement("""update tasks set status = ? where id = ?""");
        sql.setString(1, status)
        sql.setInt(2, taskID)
        sql.executeUpdate();

        // Basically checking if there is anything for this task yet.
        sql = c.prepareStatement("""select * from task_status_changes where task_id = ? order by status_start_time desc limit 1""")
        sql.setInt(1, taskID);
        var result = sql.executeQuery();
        var prev: Option[java.sql.Timestamp] = None;
        var flag = 0;
        var cycleNumber = 0;
        if (result.next()) { // if yes, we're updating previous data, and inserting new
            cycleNumber = result.getInt("cycle_number")
            prev = Some(result.getTimestamp("status_start_time"))
            flag = 0
        } else { // if no previous data, we just insert half-empty row;
            flag = 1;
        }

        var now = device.timestamp
        // writing half-empty row with start time, and cycle number 0
        if (flag == 1) {
            sql = c.prepareStatement("""insert into task_status_changes(task_id, cycle_status, status_start_time,
                status_end_time, status_duration, cycle_number) values(?, ?, ?, ?, ?, ?)""")
            sql.setInt(1, taskID)
            sql.setString(2, status)
            sql.setTimestamp(3, new java.sql.Timestamp(now))
            sql.setNull(4, java.sql.Types.TIMESTAMP)
            sql.setNull(5, java.sql.Types.INTEGER)
            sql.setInt(6, cycleNumber)
            sql.executeUpdate();
        } else {

            // update previous
            var duration = ((now - prev.get.getTime()) / 1000).toInt
            sql = c.prepareStatement(""" update task_status_changes set status_end_time = ?, status_duration = ?
                where id = (select id from task_status_changes where task_id = ? order by status_start_time desc limit 1)""")
            sql.setTimestamp(1, new java.sql.Timestamp(now))
            sql.setInt(2, duration)
            sql.setInt(3, taskID)
            sql.executeUpdate();

            var cycleDuration = 0;
            var cycleStart: Option[java.sql.Timestamp] = None
            var cycleEnd: Option[java.sql.Timestamp] = None
            if (increaseCycle == true){
                sql = c.prepareStatement(""" select sum(status_duration) as cycle_duration, min(status_start_time) as cmin, max(status_end_time) as cmax from task_status_changes where cycle_number = ? and task_id = ? """)
                sql.setInt(1, cycleNumber)
                sql.setInt(2, taskID)
                result = sql.executeQuery()
                if(result.next()){
                    cycleDuration = result.getInt("cycle_duration")
                    cycleStart = Some(result.getTimestamp("cmin"))
                    cycleEnd = Some(result.getTimestamp("cmax"))
                }
                sql = c.prepareStatement(""" insert into cycle_durations (task_id, cycle_number, cycle_duration, cycle_start_time, cycle_end_time) values( ?, ?, ?, ?, ?) """)
                sql.setInt(1, taskID)
                sql.setInt(2, cycleNumber)
                sql.setInt(3, cycleDuration)
                sql.setTimestamp(4, cycleStart.get)
                sql.setTimestamp(5, cycleEnd.get)
                sql.executeUpdate();
                cycleNumber += 1
            }
            if (status != STATUS.COMPLETED) { // insert new half-empty
                sql = c.prepareStatement(""" insert into task_status_changes(task_id, cycle_status, status_start_time, status_end_time, status_duration, cycle_number) values(?, ?, ?, ?, ?, ? ) """)
                sql.setInt(1, taskID)
                sql.setString(2, status)
                sql.setTimestamp(3, new java.sql.Timestamp(now))
                sql.setNull(4, java.sql.Types.TIMESTAMP)
                sql.setNull(5, java.sql.Types.INTEGER)
                sql.setInt(6, cycleNumber)
                sql.executeUpdate();
            } else {
                sql = c.prepareStatement(""" insert into task_status_changes( task_id, cycle_status, status_start_time, status_end_time, status_duration,cycle_number) values(?, ?, ?, ?, ?, ? ) """)
                sql.setInt(1, taskID)
                sql.setString(2, status)
                sql.setTimestamp(3, new java.sql.Timestamp(now))
                sql.setTimestamp(4, new java.sql.Timestamp(now))
                sql.setInt(5, 0)
                /* Case when completion of task is same as completion of cycle;*/

                val stub_for_cycleNumber: Int = {
                    if (increaseCycle == true){
                        cycleNumber - 1
                    } else {
                        cycleNumber
                    }
                }
                sql.setInt(6, stub_for_cycleNumber)
                sql.executeUpdate();
            }
        }
        if (taskEnd == true) {
            sql = c.prepareStatement(""" select min(status_start_time) as start, max(status_end_time) as end from task_status_changes where task_id = ? """)
            sql.setInt(1, taskID);
            result = sql.executeQuery()
            var start: Long = 0;
            var end: Long = 0;
            if (result.next()){
                start = result.getTimestamp("start").getTime()
                end = result.getTimestamp("end").getTime()
            }
            var task_duration = ((end - start)/ 1000).toInt

            sql = c.prepareStatement(""" insert into task_results (user_id, task_id, device_id, task_start_time, task_end_time, task_duration, process_id, activity_id) values (?, ?, ?, ?, ?, ?, ?, ? ) """)
            sql.setInt(1, device.userID);
            sql.setInt(2, taskID);
            sql.setInt(3, device.deviceID);
            sql.setTimestamp(4, new java.sql.Timestamp(start));
            sql.setTimestamp(5, new java.sql.Timestamp(end));
            sql.setInt(6, task_duration);
            sql.setInt(7, device.task.processID)
            sql.setInt(8, device.task.activityID)
            sql.executeUpdate();
        }
    }

    def dispatchEvent(c: Connection, currentStatus: String, event: Int, device: Device, locationOrDeviceType: Int): (String, Boolean) = {
        /*
        val PID = device.task.processID
        val AID = device.task.activityID
        */
        val PID = device.task.processName
        val AID = device.task.activityName
        val set = Set(device.equipmentType, locationOrDeviceType)
        val key = (PID, AID, event, set)
        val newStatus = updateTask(key)
        //println("KEY:    " + key)
        if (newStatus == STATUS.UNDEFINED) {
            (currentStatus, false)
        } else if (currentStatus != STATUS.COMPLETED){
            if (currentStatus != newStatus) { // so this is the one i need atm
                val increaseCycle: Boolean = isEndOfCycle(PID, AID, currentStatus, newStatus)
                val taskEnd: Boolean = isEndOfTask(PID, AID, newStatus)
                updateDB(c, device, newStatus, increaseCycle, taskEnd)
                //println("UPDATE: " + device.deviceID + "  " + currentStatus + "  " + newStatus)
                (newStatus, true)
            } else {
                (currentStatus, false)
            }
        } else {
            (currentStatus, false)
        }
    }
    // is changed
}
