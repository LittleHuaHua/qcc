#' @rdname getQuoteData
#' @title Retrieve quote data from database
#' @description It takes in paramters regrading the InstrumentID, data frequency, desired fields for data information and time period, and returns a data frame containing all desired quote data.
#' @param id A string representing the the InstrumentID for the product
#' @param freq A string representing the user desired frequency of the data. It can be chosen from "1min", "5min", "15min", "Day", "Hour"
#' @param fields A string representing the user desired information fields of the product. If more than 1 field is desired, separate them by blank space
#' @param from A string in the form of "yyyy.mm.dd" representing the start date desired
#' @param to A string in the form of "yyyy.mm.dd" representing the end date desired
#' @usage qcc <- QCC(url, jwt)
#' qcc$getQuoteData(id, freq, fields, from, to)
#' @return A data frame containing the user desired data frame from the corresponding api address filterd by all given parameters of the function
#' @export

getQuotData = function(id, freq, fields, from, to) {
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
