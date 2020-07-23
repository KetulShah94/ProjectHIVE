package com.mcit.winter2020.hadoop.hive

case class EnrichedTrip(trip: Trip, route: Route, calendar: Calender)

object EnrichedTrip {

  def formatOutput(trip: Trip, route: Route, calendar: Calender): String = {
    trip.routeId + "," +
      trip.serviceId + "," +
      trip.tripId + "," +
      trip.tripHeadSign + "," +
      trip.directionId + "," +
      trip.shapeId + "," +
      trip.wheelchairAccessible + "," +
      trip.noteFr.getOrElse("") + "," +
      trip.noteEn.getOrElse("") + "," +
      route.routeLongName
  }

}