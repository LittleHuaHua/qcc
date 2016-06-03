Installing Instruction

To have the access to the module 'qcc' in R, based on the operating sysytem, follow the next steps:

1. On Ubuntu or Mac OS X: 
- Check whether the packages "RJSONIO" and "stringr" are already installed in your R. If not, choose CRAN and install "RJSONIO" and "stringr" first.
- Download the tarball file 'qcc_1.0.tar.gz' to your desired directory
- Open terminal, change the work directory to the desired one, and enter the command: 
	$ R CMD INSTALL	qcc_1.0.tar.gz
- Now, you can use the package 'qcc' in any R with the command 
	> library(qcc)

2. On Windows: 
- Check whether the packages "RJSONIO" and "stringr" are already installed in your R. If not, choose CRAN and install "RJSONIO" and "stringr" first.
- Download the tarball file 'qcc_1.0.tar.gz' to your desired directory
- In your R console, in the menu bar, choose 
	Packages -> Install package(s) from local files, 
	and open the tarball file to install the 'qcc' package
- Now, you can use the package 'qcc' in any R with the command 
	> library(qcc)
###########################
to use our custom library

qcc <- QCC(url, jwt)
where url is the base url of the QCC server instance
and jwt is your token, e.g.
qcc <- QCC("http://analytics.qcc-x.com", "SD(G&D*SGYSDHGJ#HTMNDG")

then to retrieve fundamental data, do
qcc$getFundamentalData(id, ...)
