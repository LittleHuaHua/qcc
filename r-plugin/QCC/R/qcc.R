#' @name QCC
#' @aliases QCC
#' @title Entry Point of the 'QCC' package
#' @description Function QCC(url, jwt) provides the access to the main functions of extracting data in the package
#' @param url A string representing the base url of the QCC server instance
#' @param jwt A string representing user's token provided to access QCC database
#' @usage QCC(url, jwt)
#' @include getFundamentalData.R
#' @include getQuoteData.R
#' @export
QCC <- function(url, jwt) {
  thisEnv <- environment()

  url_ <- url
  jwt_ <- jwt

  #remove trailing /
  length <- str_length(url_)
  last <- substr(url_, length, length)
  if (last == '/') url_ = substr(url_, 1, length - 1)

  me <- list (
    thisEnv = thisEnv,
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
    },
    getquoteData = function(id, freq, fields, from, to) {
      if (missing(id)) stop("no product specified.")

      if (missing(freq)) freq <- ""
      else freq <- str_c("/", freq)

      if (missing(fields)) {
        fields <- ""
        jwt_ <- str_c("?jwt=", jwt_)
      }
      else {
        fields_l <- unlist(strsplit(fields, " "))
        fields <- fields_l[1]
        col_name <- c(fields)
        i <- 2
        while(i <= length(fields_l)) {
          fields <- str_c(fields, "%20", fields_l[i])
          col_name <- c(col_name, fields_l[i])
          i <- i+1
        }
        fields <- str_c("?fields=", fields)
      }

      if (missing(from)) from <- ""
      else from <- str_c("&from=", from)

      if (missing(to)) to <- ""
      else to <- str_c("&to=", to)

      url <- str_c(url_, id, freq, jwt_, fields, from, to)
      data <- fromJSON(url, nullValue=NULL)
      l <- length(data)
      datatable <- matrix(unlist(data), nrow=l, byrow=TRUE)
      colnames(datatable) <- col_name
      return(datatable)
    }
  )

  assign('this', me, envir=thisEnv)

  class(me) <- append(class(me), "QCC")
  return(me)
}

#qcc <- QCC("http://analytics.qcc-x.com", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MSwiaWF0IjoxNDYyOTk3ODY3fQ.J9nVYvU3KRrX4dUao5f47nJo5roAYnwZ2UbOaY5mIlI")
