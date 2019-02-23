package edu.tub.berlin.dima.bdapro.flink.benchmark.models

case class Person(personId: Long, name: String,
                  email: String, creditCard: String, city: String, state: String, eventTime: Long, processTime: Long)
