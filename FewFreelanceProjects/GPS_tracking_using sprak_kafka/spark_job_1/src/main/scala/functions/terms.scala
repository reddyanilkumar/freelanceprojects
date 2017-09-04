package aw
import play.api.libs.json.Json
import com.vividsolutions.jts.geom.Geometry

/*
This file contains defenitions of classes and objects
*/

object terms{
    /** type of event
    * event_type = 0 if devices engage
    * event_type = 1 if devices disengage
    * event_type = 2 if device enters some Location
    * event_type = 3 if device leaves some location
    * event_type = 5 if coordinates now and before are same
    */
    object EVENT{
        val ENGAGE = 0
        val DISENGAGE = 1
        val ONLOC = 2

        val ENTER_LOC100 = 11
        val ENTER_BOUNDARY = 12
        val LEAVE_LOC100 = 13
        val LEAVE_BOUNDARY = 14

        val IDLE = 5
        val NOT_IDLE = 6
    }

    object PROCESS_STATUS{
        val ONGOING = 1
        val NOMORE = 2
        val IDLE = 3
    }

    object STATUS{
        val LOADING = "Loading"
        val DRIVING = "Driving"
        val IDLE = "Idle"
        val UNLOADING = "Unloading"
        val OPERATING = "Operating"
        val COMPLETED = "Completed"
        val UNDEFINED = "Undefined"
    }

    case class Task(
        var taskID: Int,
        var taskStatus: String,
        var taskDestinationID: Int,
        var taskPickupID: Int,
        var taskFieldID: Int,
        var processID: Int,
        var activityID: Int,
        var processName: String,
        var activityName: String
    ){
        override def toString(): String = {
            "ID: " + taskID + " Status: " + taskStatus + " DestID: " + taskDestinationID +
            " PickUpID: " + taskPickupID + " PID: " + processID + " AID: " + activityID
        }
    }

    case class GPSProperties(
        gps_lat: Double,
        gps_long: Double,
        gps_alt: Double,
        gps_spd: Double,
        acc_x: Double,
        acc_y: Double,
        acc_z: Double,
        bearing: String
    ){
        override def toString(): String = {
            "Lat: " + gps_lat + " Long: " + gps_long
        }
    }
    case class Device(
        deviceID: Int,
        userID: Int,
        timestamp: Long,
        statusCode: Int,
        equipmentID: Int,
        geoData: GPSProperties,
        var equipmentType: Int,
        var task: Task,
        var locationID: Int,
        var inActivity: Boolean,
        // Anil 's
        var width: Int,
        var isInBoundary: Boolean
        // Anil 's
    ){
        override def toString(): String = {
            "\nID: " + deviceID + " Active: " + inActivity +
            "\nTask: " + task + "\nGeo: " + geoData + " Loc: " + locationID
        }
    }

    implicit val TaskReader = Json.reads[Task]
    implicit val GPSPropertiesReader = Json.reads[GPSProperties]
    implicit val DeviceReader = Json.reads[Device]


    /*Pair of two devices which are engaged*/
    case class activePair(
        var timeClose: Int,
        var timeStart: Long,
        var timeEnd: Long,
        var devices: List[Device],
        var timeWithoutData: Int
    ){
        override def toString(): String =  {
            "Close: " + timeClose + " TWD: " + timeWithoutData +
            " Devices: " + devices
        }
    }

    /*Device which is active or idle*/
    case class activeDevice(
        var timeActive: Int,
        var timeStart: Long,
        var timeEnd: Long,
        var device: Device,
        var locationID: Int,
        var locationType: Int,
        var idle: Boolean,
        var timeWithoutData: Int
    ){
        override def toString(): String = {
            "Active: " + timeActive + " TWD: " + timeWithoutData + " Idle: " + idle +
            " Device: " + device +
            "\nLocation: " + locationID + " LocationType: " + locationType
        }
    }

    case class LocMGR(var now: Int = -999, var prev: Int = -999){
        def +=(newid: Int): Unit = {
            prev = now;
            now = newid;
        }
    }

    case class Location(
        locationID: Int,
        locationName: String,
        locationShape: Geometry,
        locationType: String,

        ///////////////////////////////////////////////////////////////////////////////////////
        // Anil 's
        locationAcres: Double,
        // Anil 's
        ///////////////////////////////////////////////////////////////////////////////////////

        // for cases, when it is not a field, but location with Lat/Long coords;

        latitude: Double,
        longitude: Double

    )



}
