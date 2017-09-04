package aw
import aw.engage._
import aw.functions._
import aw.terms._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.geotools.referencing.{CRS, GeodeticCalculator}
import org.geotools.geometry.jts.JTS
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming._
import org.postgresql.Driver
import java.sql.Connection
import java.sql.DriverManager
import play.api.libs.json._
import scala.collection.mutable.{Map, ArrayBuffer}
import scala.math.{min, max}
import scala.util.control.Breaks._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder

object Kafka {
    val LIMIT = 60; // 60 seconds for engagement
    // Map of devices which are close, that persists across batches
    var activePairs = Map[Set[Int], activePair]();
    var activeDevices = Map[Int, activeDevice]();
    var idleDevices = Map[Int, activeDevice]();
    //Map of device_id and its location manager
    var locationManager = Map[Int, LocMGR]()
    // Map of device_id and location, but this is identified only after confirmed activity on some location;
    var lastVisited = Map[Int, Int]()
    /* PREFETCHING LOCATIONS DATA */
    var locs = Map[Int, Location]()
    val crs = CRS.decode("EPSG:4326");
    val gc = new GeodeticCalculator(crs);
    val wkt = new WKTReader()
    // Map of statuses for each device
    var statuses = Map[Int, String]()

    // Map of devices to detect change in GPS coordinates
    var devices_GPS = Map[Int,String]()
    var devices_OPTIME = Map[Int,Long]()
    var devices_IDLETIME = Map[Int,Long]()

    /*This function finds pairs of devices, which are close enough, and
    * are engageable, "remembers" them, and tracks time
    * or tracks 1 device on field*/
    var interval = 1;
    def func(devices: List[Device], radius: Double, timedelta: Int, dbdata : List[String]): Unit = {
        val driver = "org.postgresql.Driver" //
        val url = "jdbc:postgresql://"  + dbdata(0) //
        val user = dbdata(1) //
        val password = dbdata(2) //
        Class.forName(driver)
        var c = DriverManager.getConnection(url, user, password) //

        var activePairsNow = new ArrayBuffer[Set[Int]] //active pairs of devices+ on this batch
        var activeDevicesNow = new ArrayBuffer[Int] // active devices on this batch
        var idleDevicesNow = new ArrayBuffer[Int] //idle devices on this batch;

        // FETCHING DATA FOR DEVICES AND CHECKING THEIR POSITIONS
        for (i <- 0 until devices.size) { // loop all devices
            var sql = c.prepareStatement("""select task_id, activity_id, activity_name, process_name, process_id, dropoff_location_id, pickup_location_id, field_id, width,
                status, equipment_type_id from view_user_tasks where device_id = ? and status != 'Completed' limit 1""")
                // width,

            sql.setInt(1, devices(i).deviceID)
            var result = sql.executeQuery();
            if (result.next()) {
                devices(i).task.processID = result.getInt("process_id")
                devices(i).task.activityID = result.getInt("activity_id")
                devices(i).task.taskID = result.getInt("task_id")
                devices(i).task.processName = result.getString("process_name")
                devices(i).task.activityName = result.getString("activity_name")
                // Anil 's
                devices(i).width = result.getInt("width")
                // Anil 's

                // A workaround for different task types,
                // where we have or field_id OR pickup/dropoff ids
                // getInt returns 0 in case of null in database
                var fieldID = result.getInt("field_id")
                var dropoffID = result.getInt("dropoff_location_id");
                var pickupID = result.getInt("pickup_location_id");
                if (fieldID == 0 && dropoffID != 0 && pickupID != 0) {
                    fieldID = -200
                } else if (fieldID != 0 && dropoffID == 0 && pickupID == 0){
                    dropoffID = -200;
                    pickupID = -200;
                }
                devices(i).task.taskDestinationID = dropoffID
                devices(i).task.taskPickupID = pickupID
                devices(i).task.taskFieldID = fieldID
                devices(i).task.taskStatus = result.getString("status")
                devices(i).equipmentType = result.getInt("equipment_type_id")
                statuses(devices(i).deviceID) = result.getString("status")
            }
            var point = wkt.read(makePoint(devices(i).geoData.gps_long, devices(i).geoData.gps_lat)) // Device COORDS
            var tmpID = -1
            breakable {
                for ((id, loc) <- locs) {
                    var dist: Double = 0 ;
                    if (loc.locationType == "field") { // for fields represented by polygon
                        var polygon = loc.locationShape
                        var closest = DistanceOp.nearestPoints(polygon, point) // find closes point ON polygon TO the point
                        gc.setStartingPosition(JTS.toDirectPosition(closest(0), crs)) //
                        gc.setDestinationPosition(JTS.toDirectPosition(point.getCoordinate(), crs))
                        val dist = gc.getOrthodromicDistance()
                        if (dist <= 100) { // less than 400 meters
                            //println(devices(i).deviceID + " is near to" + id)
                            devices(i).locationID = id
                            tmpID = 0
                            if (dist == 0) {//on location
                                devices(i).locationID = id
                                devices(i).isInBoundary = true
                                tmpID = 1
                                if(lastVisited.get(devices(i).deviceID) == None){
                                    lastVisited(devices(i).deviceID) = id
                                }
                            break;
                            }
                            if(lastVisited.get(devices(i).deviceID) == None){
                                lastVisited(devices(i).deviceID) = id
                            }
                        }
                    } else { //for LOCATIONS WITH LAT/LONG coords
                        dist = haversine( loc.latitude, loc.longitude, devices(i).geoData.gps_lat, devices(i).geoData.gps_long)
                        if (dist <= 0.1){
                            //println(devices(i).deviceID + " is near to location " + id)
                            devices(i).locationID = id
                            tmpID = 1
                            devices(i).isInBoundary = true
                            if (lastVisited.get(devices(i).deviceID) == None){
                                lastVisited(devices(i).deviceID) = id
                            }
                        break;
                        }
                    }
                }
            }
            if(lastVisited.get(devices(i).deviceID) == None){
                lastVisited(devices(i).deviceID) = -1
            }
            if (locationManager.get(devices(i).deviceID) == None) {
                locationManager += (devices(i).deviceID -> LocMGR(tmpID, -999))
            } else {
                locationManager(devices(i).deviceID) += tmpID
            }
        }
        for (i <- 0 until devices.size) { // loop through devices
            for (j <- i + 1 until devices.size) { // loop through devices
                if (devices(i).task.taskID != 0 && devices(j).task.taskID != 0){
                    var sameproc = { devices(i).task.processName == devices(j).task.processName } // do devices have same task?
                    var dist = haversine(
                        devices(i).geoData.gps_lat, devices(i).geoData.gps_long,
                        devices(j).geoData.gps_lat, devices(j).geoData.gps_long
                    )
                    if (dist < radius && sameproc) { // if the distance is closer than some RADIUS
                        var pair = Set(devices(i).deviceID, devices(j).deviceID) // add their ids to 'set' (it is a pair of two IDs)
                        devices(i).inActivity = true //
                        devices(j).inActivity = true // these two are in activity, so no need to check them with field; (w)
                        activePairsNow += pair
                        if (activePairs.get(pair) == None) {
                            activePairs += (pair -> activePair(timeClose = 0,
                                timeStart = min(devices(i).timestamp, devices(j).timestamp),
                                timeEnd = max(devices(i).timestamp, devices(j).timestamp),
                                devices = List(devices(i), devices(j)),
                                timeWithoutData = - timedelta))
                        } else {
                            activePairs(pair).timeClose += timedelta
                            activePairs(pair).devices = List(devices(i), devices(j))
                            activePairs(pair).timeEnd = max(devices(i).timestamp, devices(j).timestamp)
                            activePairs(pair).timeWithoutData = - timedelta
                        }
                        if( activePairs.get(pair).get.timeClose >= LIMIT / 2){
                            lastVisited(devices(i).deviceID) = devices(i).locationID
                            lastVisited(devices(j).deviceID) = devices(j).locationID
                        }
                    }
                }
            } // loop through devices J
        } // loop through devices I

        for (i <- 0 until devices.size){
            ///////////////////////////////////////////////////////////////////////////////////////
            // Anil 's
            var etc: Double = 0
            var area: Double = 0
            var fieldAcres: Double = 0;
            var optime: Long = 0;
            var idletime: Long = 0;
            // Anil 's
            ///////////////////////////////////////////////////////////////////////////////////////
            if (devices(i).task.taskID != 0) {
                // This locType represents 'dropOffLocation' or 'pickUpLocation' or 'Field'
                // Because just leaving/entering/being_on some location would give nothing;
                val locType = {
                    if (devices(i).locationID == devices(i).task.taskPickupID ) {-1
                    } else if (devices(i).locationID == devices(i).task.taskDestinationID) {-2
                    } else if (devices(i).locationID == devices(i).task.taskFieldID){-3
                    } else if (devices(i).locationID == -1) {
                        if (devices(i).task.taskPickupID == lastVisited.get(devices(i).deviceID).get ) {-1
                        } else if (devices(i).task.taskDestinationID == lastVisited.get(devices(i).deviceID).get) {-2
                        } else if (devices(i).task.taskFieldID == lastVisited.get(devices(i).deviceID).get ){-3
                        } else { -4 } // not on any location, like, in the middle of SOMEwhere :)
                    } else { 0 } // not pickup and not dropoff but some other; [complete for truck at harvesting]
                }
                val lMan = locationManager(devices(i).deviceID)
                if (lMan.prev == -1 && lMan.now == 0) {
                    //println("Entering by 400m")
                    statuses(devices(i).deviceID) = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.ENTER_LOC100, devices(i), locType)._1
                } else if (lMan.prev == 0 && lMan.now == 1) {
                    //println("Entering by border")
                    statuses(devices(i).deviceID) = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.ENTER_BOUNDARY, devices(i), locType)._1
                } else if (lMan.prev == -1 && lMan.now == 1) {
                    statuses(devices(i).deviceID) = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.ENTER_BOUNDARY, devices(i), locType)._1
                    statuses(devices(i).deviceID) = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.ENTER_LOC100, devices(i), locType)._1
                }
                if (devices_GPS.get(devices(i).deviceID) == None) {
                    devices_GPS(devices(i).deviceID) = devices(i).geoData.gps_lat + ":" + devices(i).geoData.gps_long
                    //////////////////////////////////////////////////////////////////////////////////////
                    // Anil 's
                    devices_OPTIME(devices(i).deviceID) = devices(i).timestamp
                    devices_IDLETIME(devices(i).deviceID) = devices(i).timestamp
                    // Anil 's
                    //////////////////////////////////////////////////////////////////////////////////////
                } else if (devices_GPS.get(devices(i).deviceID).get != devices(i).geoData.gps_lat + ":" + devices(i).geoData.gps_long){ // NOT IDLE
                    val test = engageable_with_locations(
                        devices(i).equipmentType,  if (locs.get(devices(i).locationID) != None) {locs.get(devices(i).locationID).get.locationType
                        } else { "ErrorType" })
                    if (devices(i).inActivity == false && test && devices(i).isInBoundary == true) {
                        devices(i).inActivity = true
                        activeDevicesNow += devices(i).deviceID
                        if (activeDevices.contains(devices(i).deviceID) == false) {
                            activeDevices += (devices(i).deviceID -> activeDevice(timeActive = 0,
                                timeStart = devices(i).timestamp,  timeEnd = devices(i).timestamp,
                                device = devices(i), locationID = devices(i).locationID,
                                locationType = locType, idle = false,
                                timeWithoutData = - timedelta ))
                        } else {
                            activeDevices(devices(i).deviceID).timeActive += timedelta
                            activeDevices(devices(i).deviceID).timeEnd = devices(i).timestamp
                            activeDevices(devices(i).deviceID).idle = false
                            activeDevices(devices(i).deviceID).device = devices(i)
                            activeDevices(devices(i).deviceID).timeWithoutData = - timedelta
                        }
                        if (activeDevices.get(devices(i).deviceID).get.timeActive >= LIMIT / 2) {
                            lastVisited(devices(i).deviceID) = devices(i).locationID
                        }
                        //////////////////////////////////////////////////////////////////////////////////////
                        // so, if the device is on field, and could do something on field
                         if(locs.get(devices(i).locationID).get.locationType == "field" && test) {
                            // Anil 's
                            fieldAcres = locs.get(devices(i).locationID).get.locationAcres
                            // Anil 's
                        }
                        ///////////////////////////////////////////////////////////////////////////////////////
                    }
                } else {// IDLE
                    if (devices(i).inActivity == false) {
                        idleDevicesNow += devices(i).deviceID
                        if (idleDevices.contains(devices(i).deviceID) == false) {
                            idleDevices += (devices(i).deviceID -> activeDevice(timeActive = 0,
                                timeStart = devices(i).timestamp, timeEnd = devices(i).timestamp,
                                device = devices(i), locationID = devices(i).locationID,
                                locationType = locType, idle = true,
                                timeWithoutData = - timedelta ))
                        } else {
                            idleDevices(devices(i).deviceID).timeActive += timedelta
                            idleDevices(devices(i).deviceID).timeEnd = devices(i).timestamp
                            idleDevices(devices(i).deviceID).device = devices(i)
                            idleDevices(devices(i).deviceID).idle = true
                            idleDevices(devices(i).deviceID).timeWithoutData = - interval
                        }
                        if (idleDevices.get(devices(i).deviceID).get.timeActive >= LIMIT / 2) {
                            lastVisited(devices(i).deviceID) = devices(i).locationID
                        }
                    }
                    ///////////////////////////////////////////////////////////////////////////////////////

                    optime = devices(i).timestamp - devices_OPTIME(devices(i).deviceID)
                    devices_OPTIME(devices(i).deviceID) = devices(i).timestamp
                    //IDLETIME()
                    if (optime/1000 <= interval) {
                        optime = 0
                        if((devices(i).geoData.acc_x+devices(i).geoData.acc_y+devices(i).geoData.acc_z)>0.6){
                            idletime = devices(i).timestamp - devices_IDLETIME(devices(i).deviceID)
                            devices_IDLETIME(devices(i).deviceID) = devices(i).timestamp
                        }
                    }
                    // Anil 's
                    ///////////////////////////////////////////////////////////////////////////////////////
                }
                if (lMan.prev == 1 && lMan.now == 0) {
                    //println(devices(i).deviceID + "Leaving by border")
                    var eventResult = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.LEAVE_BOUNDARY, devices(i), locType)
                    if (eventResult._2 == true){
                        for (key <- activePairs.keys){
                            if (key.contains(devices(i).deviceID)){
                                var pair = activePairs(key).devices
                                if (pair(0).deviceID == devices(i).deviceID) {
                                    statuses(pair(1).deviceID) = dispatchEvent(c, statuses(pair(1).deviceID), EVENT.DISENGAGE, pair(1), pair(0).equipmentType)._1
                                } else {
                                    statuses(pair(0).deviceID) = dispatchEvent(c, statuses(pair(0).deviceID), EVENT.DISENGAGE, pair(0), pair(1).equipmentType)._1
                                }
                                activePairs.remove(key)
                            }
                        }
                        activeDevices.remove(devices(i).deviceID)
                        idleDevices.remove(devices(i).deviceID)
                    }
                    statuses(devices(i).deviceID) = eventResult._1
                } else if (lMan.prev == 0 && lMan.now == -1) {
                    //println(devices(i).deviceID + "Leaving by 400m")
                    var eventResult = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.LEAVE_LOC100, devices(i), locType)
                    if (eventResult._2 == true){
                        if (lastVisited.contains(devices(i).deviceID)){
                            lastVisited.remove(devices(i).deviceID)
                        }
                        for (key <- activePairs.keys){
                            if (key.contains(devices(i).deviceID)){
                                var pair = activePairs(key).devices
                                if (pair(0).deviceID == devices(i).deviceID) {
                                    statuses(pair(1).deviceID) = dispatchEvent(c, statuses(pair(1).deviceID), EVENT.DISENGAGE, pair(1), pair(0).equipmentType)._1
                                } else {
                                    statuses(pair(0).deviceID) = dispatchEvent(c, statuses(pair(0).deviceID), EVENT.DISENGAGE, pair(0), pair(1).equipmentType)._1
                                }
                                activePairs.remove(key)
                            }
                        }
                        activeDevices.remove(devices(i).deviceID)
                        idleDevices.remove(devices(i).deviceID)
                    }
                    statuses(devices(i).deviceID) = eventResult._1
                } else if (lMan.prev == 1 && lMan.now == -1) {
                    //println(devices(i).deviceID + "Leaving by 400m")
                    var eventResult = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.LEAVE_LOC100, devices(i), locType)
                    var eventResult1 = dispatchEvent(c, statuses(devices(i).deviceID), EVENT.LEAVE_BOUNDARY, devices(i), locType)
                    for (result <- List(eventResult, eventResult1)){
                        if (result._2 == true) {
                            if (lastVisited.contains(devices(i).deviceID)) {
                                lastVisited.remove(devices(i).deviceID)
                            }
                            for (key <- activePairs.keys) {
                                if (key.contains(devices(i).deviceID)){
                                    var pair = activePairs(key).devices
                                    if (devices(i).deviceID == pair(0).deviceID) {
                                        statuses(pair(1).deviceID) = dispatchEvent(c, statuses(pair(1).deviceID), EVENT.DISENGAGE, pair(1), pair(0).equipmentType)._1
                                    } else if (devices(i).deviceID == pair(1).deviceID) {
                                        statuses(pair(0).deviceID) = dispatchEvent(c, statuses(pair(0).deviceID), EVENT.DISENGAGE, pair(0), pair(1).equipmentType)._1
                                    }
                                    activePairs.remove(key)
                                }
                            }
                            activeDevices.remove(devices(i).deviceID)
                            idleDevices.remove(devices(i).deviceID)
                        }
                    }
                    statuses(devices(i).deviceID) = {
                        if( eventResult._1 == statuses(devices(i).deviceID) ){
                            eventResult1._1
                        } else{
                            eventResult._1
                        }
                    }
                }
                devices_GPS(devices(i).deviceID) = devices(i).geoData.gps_lat + ":" + devices(i).geoData.gps_long

                //////////////////////////////////////////////////////////////////////////////////////
                // Anil 's

                if (devices(i).geoData.gps_spd != 0 && fieldAcres != 0) {
                    /*Little comment on this,
                    since i was trying to minimize the queries count,
                    i've added this info to first query to Tasks;
                    */
                    val width: Double = getWidthField(devices(i).userID, dbdata) // <- should be removed
                    println("width :"+width)
                    println("Field acres :"+fieldAcres)
                    val (etc1, area1) = estimatedTimeOfCompletion(devices(i).geoData.gps_spd, width, fieldAcres)
                    println("Estimated time of completion for equipment with device-id "+devices(i).userID+" is "+etc+" min and area: "+area )
                    etc = etc1
                    area = area1
                }
                println("OPTIME :"+optime)
                println("IDLETIME :"+idletime)
                //val task_id = getTaskEquipment(devices(i).device_id, c)
                println("user_id :"+devices(i).userID +",task_id :"+devices(i).task.taskID+",equipment_id :"+devices(i).equipmentID)
                insertToDB(devices(i), etc, area, optime, idletime, c)
                // Anil 's
                //////////////////////////////////////////////////////////////////////////////////////
            }
            //moved it here, so gps is dumped in any case;
            insertToDB(devices(i), etc, area, optime, idletime, c)
        }
        for (deviceKey <- activeDevices.keys){
            if (activeDevicesNow.contains(deviceKey) == false) {
                activeDevices(deviceKey).timeWithoutData += LIMIT + timedelta
            }
            if (activeDevices(deviceKey).timeWithoutData >= LIMIT) {
                activeDevices.remove(deviceKey)
            } else if (activeDevices(deviceKey).timeActive >= 0) {
                //println("Active on Loc " + EVENT.ONLOC)
                if (idleDevices.contains(deviceKey)) {
                    idleDevices.remove(deviceKey)
                }
                statuses(deviceKey) = dispatchEvent(c, statuses(deviceKey),EVENT.ONLOC, activeDevices(deviceKey).device, activeDevices(deviceKey).locationType)._1
            }
        }
        for (deviceKey <- idleDevices.keys){
            if(idleDevicesNow.contains(deviceKey) == false){
                idleDevices(deviceKey).timeWithoutData += LIMIT + timedelta
            }
            if (idleDevices(deviceKey).timeWithoutData >= LIMIT){
                statuses(deviceKey) = dispatchEvent(c, statuses(deviceKey), EVENT.NOT_IDLE, idleDevices(deviceKey).device, idleDevices(deviceKey).locationType)._1
                idleDevices.remove(deviceKey)
            } else if (idleDevices(deviceKey).timeActive >= 0){
                statuses(deviceKey) = dispatchEvent(c, statuses(deviceKey), EVENT.IDLE, idleDevices(deviceKey).device, idleDevices(deviceKey).locationType)._1
                if (activeDevices.contains(deviceKey)) {
                    activeDevices.remove(deviceKey)
                }
            }
        }
        for (pair <- activePairs.keys) {
            val devices = activePairs(pair).devices
            if (activePairsNow.contains(pair) == false) {// the data for this pair did not come this batch
                activePairs(pair).timeWithoutData += LIMIT + timedelta
            }
            if (activePairs(pair).timeWithoutData >= LIMIT){ // there was no data for LIMIT time for this pair
                statuses(devices(0).deviceID) = dispatchEvent(c, statuses(devices(0).deviceID),EVENT.DISENGAGE, devices(0), devices(1).equipmentType)._1
                statuses(devices(1).deviceID) = dispatchEvent(c, statuses(devices(1).deviceID),EVENT.DISENGAGE, devices(1), devices(0).equipmentType)._1
                activePairs.remove(pair)
            } else { // otherways, like it is still valid
                if (activePairs(pair).timeClose >= LIMIT){
                    // event engage
                    statuses(devices(0).deviceID) = dispatchEvent(c, statuses(devices(0).deviceID),EVENT.ENGAGE, devices(0), devices(1).equipmentType)._1
                    statuses(devices(1).deviceID) = dispatchEvent(c, statuses(devices(1).deviceID),EVENT.ENGAGE, devices(1), devices(0).equipmentType)._1
                }
            }
        }
        /*
        println("================DEVICES================")
        devices.foreach(println)
        println("=======================================\n")
        */

        /*
        println("-= T R A C K I N G =-")
        println("PAIRS:\n")
        println(activePairs)
        println("\nDEVICES:\n")
        println(activeDevices)
        */

        c.close()
        //println(statuses)

        var discard = new ArrayBuffer[Int]
        statuses.foreach{
            e => if (e._2 == STATUS.COMPLETED) { discard += e._1 }
        }
        discard.foreach{
            e =>
            statuses.remove(e)
            lastVisited.remove(e)
        }
    }

    def main(args: Array[String]){
        /**
        This program is used with command line arguments, these are as follows:
        interval topic zkQuorum consumerGroup numThreads postgres_address postgres_user postgres_password radius
        the programm should be launched as:
        spark-submit --class aw.Kafka kafka-assembly-1.0.jar interval topic zkQuorum consumerGroup numThreads postgres_address postgres_user postgres_password radius
        */
        if (args.length != 9) {
            println("Command line arguments are incorrect")
            args.foreach(println)
            println(args.length)
            System.exit(1)
        }
        val interval = args(0).toInt // seconds 1
        val kafkaTopics = args(1) // test
        val zkQuorum = args(2) //localhost:2181
        val group = args(3) // kafka consumer group
        val numThreads = args(4) //
        val postgre_addr = args(5)  //localhost:5432/wisran"
        val postgre_usr = args(6) // wisran
        val postgre_pass = args(7) // wisran123
        val radius = args(8).toDouble
        // PostgreSQL address, user, and password together
        val access = List(postgre_addr, postgre_usr, postgre_pass);
        val driver = "org.postgresql.Driver" //
        val url = "jdbc:postgresql://"  + access(0) //
        val user = access(1) //
        val password = access(2) //
        Class.forName(driver)
        var c = DriverManager.getConnection(url, user, password)//

        /* PREFETCHING LOCATIONS DATA */
        var sql = c.prepareStatement(""" select location_id, latitude, longitude, name, st_asText(field_shape)as shape, location_type from view_locations_fields""")
        // acres,
        var res = sql.executeQuery();

        while(res.next()){
            var id = res.getInt("location_id")
            var name = res.getString("name")
            var shp = res.getString("shape")
            /* var geom: Option[Geometry] = if (shp != null){
                Some(wkt.read(shp))
            } else {
                None
            }*/
            var geom = if (shp != null) {
                wkt.read(shp)
            } else {
                null
            }
            var ltype = res.getString("location_type")

            //var acres = res.getDouble("acres") // will be 0 for null
            var acres: Double = 0;
            var latitude = res.getDouble("latitude") // will be 0 for null
            var longitude = res.getDouble("longitude") // will be 0 for null
            if (ltype == "field" && shp == null) { // if field but with no shape
            } else if (ltype != "field" && latitude == 0 || longitude == 0){ // if not field with no coords
            }else {
                locs += ( id -> Location( id, name, geom, ltype, acres, latitude, longitude) )
            }
        }

        //locs.foreach(println)
        //Spark config and context
        val spark_conf = new SparkConf().setAppName("Kafka").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(spark_conf)
        // This is used to remove system messages of Spark from console output;
        sc.setLogLevel("ERROR")
        // create streaming context;
        val ssc = new StreamingContext(sc,Seconds(interval))
        // split kafkaTopics string to topicMap
        val topicMap = kafkaTopics.split(",").map((_, numThreads.toInt)).toMap
        // Parameters for Kafka consumer, used to reset offsets, and connect to zookeeper quorum;
        val kafkaParams = scala.collection.immutable.Map[String, String](
            "zookeeper.connect" -> zkQuorum, "group.id" -> group, "auto.commit.interval.ms" ->"100",
            "zookeeper.connection.timeout.ms" -> "10000",
            "auto.offset.reset" -> "smallest"
        )
        // Create Kafka stream;
        val lines = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
            ssc, kafkaParams, topicMap, org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER).map(_._2)
        // parse each line of json into Device;
        // More precisely it is turned into a single-item list of Device
        var entries = lines.map(parse)
        // reduce to List of Objects
        var reduced = entries.reduce(reduceDevices)
        // aggregate these items, taking last of rows for each object
        var aggregated = reduced.map(aggregate)
        // pass List to function that classifies the activities
        // rather tracks cycles now, since we do not have any classification atm
        var result = aggregated.map(e => func(e, radius, interval, access))
        result.print()
        // Force tha calculation;


        /*aggregated.foreachRDD(
        rdd => {
            try{
                func(rdd.collect()(0), radius, interval, access)
            }catch{
                case e: Exception => {println("EMPTY BATCH")}
            }
        }
        )*/
        ssc.start()
        ssc.awaitTermination()
    }
}
