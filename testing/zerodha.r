library(httr)
library(jsonlite)

### Input to the code ####
## Right click on the zerodha chart page and go to inspect element, go to network, find the chart link and extract below values

symbol_list<-read.csv("D://Trading System//Trading//R Experiement//list/Zerodha_list.csv")
path_to_store_data<-"D:/Trading System/Trading/R Experiement/"

cfduid="d302588e85bb275672e2919ad9afd28c81614742452"
enctoken="enctoken pXXF2hzBlKmgWEQjTpKhkwMNJlmQlgCa6PpNnjqvOF0A3WvYvI0BMKoL3is3oBw9fnMKdWfXE96mDR7lbd0X09OHlTDPUw=="
kf_session="QDW44iohDky7k2zpMug3o17KX6QhVU8w"
public_token="ECJOYhFy0NwfcrH1FYmJblyfdVXg0Xrg"




user_id="XE4670"

from="2021-02-05"
to="2021-04-23"

dir.create(paste0(path_to_store_data,from,"to",to))
path_to_store_data=paste0(path_to_store_data,from,"to",to,"/")
st <- as.Date(from)
en <- as.Date(to)



### Specify Interval
## minute,2minute,3minute,4minute,10minute,15minute,60minute,day

interval="minute"  
data_pull<-function(symbol_list){
i=1
for (i in i:nrow(symbol_list)){
  ID=symbol_list[i,2]
  symbol_name=symbol_list[i,1]
  print(ID)
  print(symbol_name)
  
  theDate <-st
  datalist = list()
  big_data<-data.frame()
  print(theDate)
  print(en)
  while (theDate<=en)
    
  {
    
    NextDate<-  as.Date(theDate+30)
    
    if (NextDate > as.Date(Sys.Date())){
      NextDate<-en
    }  
    dt_range=paste0(theDate,"&to=",NextDate)
    print(dt_range)  
    
    url<- paste0("https://kite.zerodha.com/oms/instruments/historical/",ID,"/",interval)
    httr::GET(
      url = url,
      add_headers(authorization=enctoken),
      query = list(
        user_id = user_id,
        oi = "1",
        from = theDate,
        to = NextDate,
        kf_session= kf_session,
        public_token=public_token,
        user_id= user_id,
        enctoken= enctoken
      )
    ) -> res
    
    dat <- httr::content(res)
    
    jsonRespText<-content(res,as="text")
    #print(jsonRespText)
    document<-fromJSON(txt=jsonRespText)
    
    x<-document[["data"]]
    y<-x[["candles"]]
    
    
    
    if (length(y) <5){
      theDate<-as.Date(theDate)+30
      next
      print("hi")
      
      print(theDate)
      
    }
    
    
    dt<-as.data.frame(document)
    dt<-dt[-1]
    colnames(dt)[1]<-"TIME"
    colnames(dt)[2]<-"Open"
    colnames(dt)[3]<-"High"
    colnames(dt)[4]<-"Low"
    colnames(dt)[5]<-"CLOSE"
    colnames(dt)[6]<-"VOLUME"
    colnames(dt)[7]<-"SYMBOL"
    dt$SYMBOL<-symbol_name
    
    dt$TIME<-gsub("\\+0530","",dt$TIME)
    
    dt$TIME<-gsub("T"," ",dt$TIME)
    dt$Date <- as.Date(dt$TIME) #already got this one from the answers above
    dt$TIME1 <- format(as.POSIXct(dt$TIME) ,format = "%H:%M:%S")
    datalist[[i]] <- dt
    #print("4")
    
    
    theDate<-as.Date(theDate)+30
    big_data = rbind(big_data,dt)
    print(theDate)
  }
  
  
  file= paste0(path_to_store_data,symbol_name,".csv",sep="")
  print(file)
  write.csv(big_data,file,row.names = F)
  
  
  print(theDate)  
}

}

undebug(data_pull)
data_pull(symbol_list)




