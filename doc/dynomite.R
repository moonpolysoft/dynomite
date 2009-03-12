analyze_dynomite <- function(path) {
	cols <- c("time", "method", "latency", "key", "host")
	files <- list.files(path)
	data <- data.frame()
	for (file in files) {
		d <- read.table(paste(path, file, sep=""), header=FALSE, sep="\t", col.names = cols)
		data <- rbind(data, d)
	}
	print(summary(data))
	graph_dynomite(data)
}

graph_dynomite <- function(data) {
	hosts <- unique(data$host)
	par(mfrow = c(ceiling(length(hosts)/2), 2), mai = c(0.5,0.1,0.1,0))
	for(i in hosts) {
		h <- hosts[i]
		hostspec <- subset(data, data$host == h)
		plot(hostspec$time, hostspec$latency, main = h)
	}
}