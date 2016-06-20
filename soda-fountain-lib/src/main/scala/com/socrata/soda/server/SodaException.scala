package com.socrata.soda.server

// exception subclass for errors which are caused by something internal
class SodaInternalException(message: String, underlying: Throwable = null) extends Exception(message, underlying)

// exception subclass for errors which are caused by e.g. bad input
class SodaInvalidRequestException(message: String) extends Exception(message)