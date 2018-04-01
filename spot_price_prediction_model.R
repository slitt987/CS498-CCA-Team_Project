#########################################
#
# Data set needs to be downloaded from
# https://www.kaggle.com/noqcks/aws-spot-pricing-market/data
# and stored in file system.
#
#########################################
setwd("~/") # Change to root directory of dataset
par(mfrow=c(2,2))
row_count = 500
if(!require(glmnet)){
  install.packages("glmnet", dependencies=TRUE)
  library(glmnet)
}
if(!require(data.table)){
  install.packages("data.table", dependencies=TRUE)
  library(data.table)
}
if(!require(MASS)){
  install.packages("MASS", dependencies=TRUE)
  library(MASS)
}
if(!require(DAAG)){
  instalibll.packages("DAAG", dependencies=TRUE)
  library(DAAG)
}

getLambda = function (bc) {
  lambda = bc$x
  lik = bc$y
  bc = cbind(lambda, lik)
  bc = bc[order(-lik), ]
  return (bc[1,1])
}

files = c("us-east-1.csv","sa-east-1.csv", "eu-west-1.csv", "eu-central-1.csv", "ca-central-1.csv",
          "ap-southeast-2.csv", "ap-southeast-1.csv","ap-northeast-1.csv" )
data = read.csv(file="us-west-1.csv", sep="," , header=T, nrows = row_count, 
                col.names = c("datetime", "instance_type", "os", "region", "price"))
for (f in files) {
  data = rbind(data, read.csv(file=f, sep="," , header=T, nrows = row_count, 
                              col.names = c("datetime", "instance_type", "os", "region", "price")))
}
price = data$price
# A Simple Linear Regression model with greedy model Search
spot_price.all.fit = lm(price ~ ., data=data, na.action=na.omit)
spot_price.1.fit = lm(price ~ 1, data=data, na.action=na.omit)
forward_greedy_model_search = step(spot_price.1.fit, direction = "forward", scope=list(upper=spot_price.all.fit,lower=spot_price.1.fit))
backward_greedy_model_search = step(spot_price.all.fit, direction = "backward") 

best_model = lm(formula = price ~ instance_type + os + region, data = data, na.action = na.omit)
summary(best_model)

# Linear regression after BoxCox transformation
transformation = boxcox(price ~ ., data = data)
lambda = getLambda(transformation)
best_boxcox_model = lm(((price)^lambda) ~ ., data=data)
