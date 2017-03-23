package com.cloudwick.sandeep.Flights

import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import org.apache.spark.rdd.RDD

object Main {
  def main(args:Array[String])
  {
    val sparkConf=new SparkConf().setAppName("Flights data")
   
    val sc=SparkContext.getOrCreate(sparkConf)
    val confFile = ConfigFactory.load()
    val ontimeInput=sc.textFile(confFile.getString("Flights.ontime"),15)
    val airportInput=sc.textFile(confFile.getString("Flights.airport"))
    val airportSplit=airportInput.map { line => val ar=line.split(","); (ar(0).trim(),ar(1).trim())}
    val carrierInput=sc.textFile(confFile.getString("Flights.carrier"))
    val carrierSplit=carrierInput.map { line => val ar=line.split(","); (ar(0).substring(1, ar(0).length()-1),ar(1).substring(1, ar(1).length()-1))}.map(x=>(x._1.trim(),x._2.trim()))
    val airport= confFile.getString("Flights.airportName")
    val ia=airportInput.map(x=>x.split(",")).map(x=> trimming(x)).filter(x=>x(1).contains(airport)).map(x=>x(0)).take(1)(0)
    val iata=ia.substring(1, ia.length()-1)
    val ontimeAirport=ontimeInput.map { line => line.split(",") }.map(x=> trimming(x)).filter { line => line(16).contains(iata) ||  line(17).contains(iata)}.map { line => Array(line(0),line(1),line(2),line(3),line(8),line(14),line(15),line(16),line(17)) }.filter { x => !x.contains("NA")}
    //Array(year, month, day of month, day of week, carrier, Arrival delay, Departure delay, Origin, Destination)
   
    val formatChange=ontimeAirport.map { arr => (arr(4),(arr(0),arr(1),arr(2),arr(3),arr(5),arr(6),arr(7),arr(8))) }.join(carrierSplit).map{x=> val y=x._2._1; Array(y._1,y._2,y._3,y._4,x._2._2,y._5,y._6,y._7,y._8)}
    //Array(year, month, day of month, day of week, carrier, Arrival delay, Departure delay, Origin, Destination)
   
    val requiredFormat=formatChange.map { x => val dayOfWeek:Int=weekOfTheYear(x(0).toInt,x(1).toInt,x(2).toInt);  Array(x(0),dayOfWeek.toString(),x(4),x(5),x(6),x(7),x(8))}.persist(StorageLevel.MEMORY_AND_DISK)
    //Array(year,week of year, carrier, Arrival delay, Departure delay, Origin, Destination)
    
    val arrivalStatistics=requiredFormat.map { x => ((x(0),x(2),x(1).toInt,x(6)),x(3).toFloat) }.filter(x=>x._1._4.contains(iata)).filter(x=>x._2>0).map(x=>((x._1._1,x._1._2,x._1._3),x._2))
    val departureStatistics=requiredFormat.map { x => ((x(0),x(2),x(1).toInt,x(5)),x(4).toFloat) }.filter(x=>x._1._4.contains(iata)).filter(x=>x._2>0).map(x=>((x._1._1,x._1._2,x._1._3),x._2))
    val statistics=arrivalStatistics.union(departureStatistics).reduceByKey(_+_).sortByKey(true).saveAsTextFile(confFile.getString("Flights.output"))
  }
  def weekOfTheYear(year:Int, month:Int, dayOfMonth:Int):Int=
  {
    val ca1:Calendar = Calendar.getInstance()
    ca1.set(year,month-1,dayOfMonth)
    ca1.get(Calendar.WEEK_OF_YEAR)
  }
  
  def trimming(arr:Array[String]):Array[String]={
    for(i <- 0 until arr.length-1)
    {
      arr(i).trim()
    }
    arr  
  }
}
