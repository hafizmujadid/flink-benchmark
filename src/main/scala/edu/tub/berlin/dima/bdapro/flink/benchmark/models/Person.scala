package edu.tub.berlin.dima.bdapro.flink.benchmark.models

/**
  * Person DTO
  * @author Hafiz Mujadid Khalid
  * @param personId
  * @param name
  * @param email
  * @param creditCard
  * @param city
  * @param state
  * @param eventTime
  * @param processTime
  */
case class Person(personId: Long, name: String,
                  email: String, creditCard: String, city: String, state: String, eventTime: Long, processTime: Long)
