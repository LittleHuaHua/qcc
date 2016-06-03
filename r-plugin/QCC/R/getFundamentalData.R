#' @rdname getFundamentalData
#' @title Retreive fundamental data from database
#' @description Extract fundamental data from the QCC database corresponding to user's input, such as time period, type of time
#' @param id A string representing the the product id
#' @param from A string in the form of "yyyy-mm-dd" representing the start date desired
#' @param to A string in the form of "yyyy-mm-dd" representing the end date desired
#' @param mon A string ranges from 01 to 12, representing the corresponding month, if several months are desired, separated by ","
#' @param day A string ranges from 01 to 31, representing the corresponding day, if several days are desired, separated by ","
#' @param hour A string ranges from 00 to 23, representing the corrsponding hour, if several hours are desired, separated by ","
#' @param dow A string ranges from 0 to 6, representing the corresponding day of week, is several day of week are desired, separated by ","
#' @param min A string ranges from 00 to 59, representing the corrsponding minute, if several minutes are desired, separated by ","
#' @param sec A string ranges from 00 to 59, representing the corrsponding second, if several seconds are desired, separated by ","
#' @usage qcc <- QCC(url, jwt)
#' qcc$getFundamentalData(id, from, to, sec, min, hour, day, mon, dow)
#' @return A data frame containing the user desired data frame from the corresponding api address filterd by all given parameters of the function
#' @export
getFundamentalData = function(id, from, to, sec, min, hour, day, mon, dow) {
  if (missing(id)) stop("no id specified")
  classificationUrl = str_c(url_, "/api/classifications?where={dataName:'", id, "'}&jwt=", jwt_)
  data <- fromJSON(classificationUrl)
  size = length(data)
  if (size == 0) stop("no matching id")
  apiAddress <- data[[1]]$apiAddress
  fields <- data[[1]]$fields

  if (missing(from)) from <- ""
  else from <- str_c("&from=", from)

  if (missing(to)) to <- ""
  else to <- str_c("&to=", to)

  if (missing(mon)) mon <- ""
  else mon <- str_c("&mon=", mon)

  if (missing(day)) day <- ""
  else day <- str_c("&day=", day)

  if (missing(dow)) dow <- ""
  else dow <- str_c("&dow=", dow)

  if (missing(hour)) hour <- ""
  else hour <- str_c("&hour=", hour)

  if (missing(min)) min <- ""
  else min <- str_c("&min=", min)

  if (missing(sec)) sec <- ""
  else sec <- str_c("&sec=", sec)

  query_url <- str_c(url_, apiAddress, "?order=date+desc&jwt=", jwt_, from, to, mon, day, dow, hour, min, sec)
  print(query_url)
  data <- fromJSON(query_url, nullValue="NA")
  l <- length(data)
  datatable <- matrix(unlist(data), nrow=l, byrow=TRUE)
  datatable <- datatable[,-c(1,3)]

  if (grepl("price", fields)) {
    colnames(datatable) <- c("date", "price")
  }
  if (grepl("quantity", fields)) {
    colnames(datatable) <- c("date", "quantity")
  }
  else {
    colnames(datatable) <- c("date", "openPrice", "highestPrice", "lowestPrice", "closePrice", "volume", "openInterest")
  }
  return(datatable)
}
