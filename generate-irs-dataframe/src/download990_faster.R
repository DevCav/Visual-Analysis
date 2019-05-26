.libPaths(c('~/MyRlibs',.libPaths()))

setwd("") #set to the directory containing src/

#update.packages(repos='http://cran.us.r-project.org')
#install.packages("xml2",repos='http://cran.us.r-project.org')
#install.packages("readr",repos='http://cran.us.r-project.org')
library(xml2)
library(readr)
library(doParallel)
library(doSNOW)

#Handle the arguments
args=commandArgs(trailingOnly=TRUE)

if ((length(args)!=2&length(args)!=3)){
    print("Incorrect arguments. Must be run as:")
    print("Rscript download990.R [year] [return type]")
    print("where [year] is a year 2013-2015 and [return type] is either 990 or 990PF")
    print("note: a third argument can be passed to specify project folder, must have trailing /")
    #stop()
}
year<-args[1]
r<-args[2]
if (length(args)==2){
    args<-c(args,"")
}
project_folder<-args[3]

#Make the cluster
system(paste0("mkdir -p log/",project_folder))
cl<-makeCluster(detectCores(),outfile=paste0("log/",project_folder,year,"10k.log"))
registerDoSNOW(cl)
clusterEvalQ(cl, .libPaths('~/MyRlibs'))

#Read in files that say what variables we'll be looking for
xml_vars<-read.delim2(paste("dta/",project_folder,"xml_vars_",year,r,".csv",sep=""),header=FALSE,sep=",",col.names="path")
xml_vars$r_var<-gsub("/","_",gsub("/text()","",xml_vars$path,fixed=TRUE))

xml_vars_recipient<-read.delim2(paste("dta/",project_folder,"xml_vars_recipient_",year,r,".csv",sep=""),header=FALSE,sep=",",col.names=c("prefix","path"))
xml_vars_recipient$r_var<-gsub("/","_",gsub("/text()","",xml_vars_recipient$path,fixed=TRUE))

#Download and read in index file of all tax returns for this year
if (!file.exists(paste("dta/",project_folder,"index",year,".csv",sep=""))){
    download.file(paste("https://s3.amazonaws.com/irs-form-990/index_",year,".csv",sep=""),paste("dta/",project_folder,"index",year,".csv",sep=""),"auto")
}
index_file<-read.delim2(paste("dta/",project_folder,"index",year,".csv",sep=""),header=TRUE,sep=",",numerals="no.loss",stringsAsFactors = FALSE)
index_file<-index_file[(index_file$RETURN_TYPE==r),]
index_file_orig<-index_file

#guardrail<-2000 #nrow(index_file)

#Set up a few core functions
download_tax_return<-function(source,destination){
    obj_id<-destination
    haystack_id<-haystack_file(destination)
    system(paste0("mkdir -p ../dta/HAYSTACK/",haystack_id))
    destination<-paste0("../dta/HAYSTACK/",haystack_id,"/",obj_id,"_public.xml")
    while ((!file.exists(destination,sep="")||file.size(destination)==0)){
        tryCatch(download_xml(source,file=destination),error=function(e){print(paste("Trying",source,"again..."))})
    }
    doc<-read_xml(destination)
    #if (file.exists(destination)) file.remove(destination)
    xml_ns_strip(doc)
    return(doc)
}

get_item_contents<-function(doc,path){
    item_contents=as_list(xml_find_all(doc,path))
    if (length(item_contents)==0) {
        item_contents<-"Unknown"
    }
    else if (length(item_contents)>1){
        item_contents<-"Multiple values"
    }
    return(unlist(item_contents))
}
#in order to download files and access them efficiently, we need to use a "haystack" structure; see https://serverfault.com/questions/95444/storing-a-million-images-in-the-filesystem
haystack_file<-function(object_id){
    haystack_id=substr(object_id,1,4)
    for (i in 5:nchar(object_id)){
        haystack_id<-paste0(haystack_id,"/",substr(object_id,i,i))
    }
    return(haystack_id)
}

increment=1000
min=0 #nrow(index_file)-nrow(index_file)%%increment-increment
max=2000#nrow(index_file)-nrow(index_file)%%increment

for (guardrail in seq(increment+min, max, increment)) {
    mod<-0
    if (guardrail==max){
        mod=nrow(index_file)%%increment
    }
    print((guardrail-increment+1):(guardrail+mod))
    #Single data point per variable per return
    xml_to_add<-foreach(i=(guardrail-increment+1):(guardrail+mod),.combine=rbind) %dopar%{
        library(xml2)
        if (i%%1000==0) print(paste0(i,"/",guardrail," records complete"))
        amazonurl<-paste("https://s3.amazonaws.com/irs-form-990/",index_file[i,"OBJECT_ID"],"_public.xml", sep="")
        xdata<-index_file[i,"OBJECT_ID"]

        doc<-download_tax_return(amazonurl,xdata)
        xml_ns_strip(doc)

        to_add<-as.data.frame(t(sapply(as.character(xml_vars[,"path"]),get_item_contents,doc=doc)))
        colnames(to_add)<-xml_vars$r_var

        to_add
    }
    index_file<-index_file[(guardrail-increment+1):(guardrail+mod),]
    index_file$OBJECT_ID<-paste0("\"",index_file$OBJECT_ID,"\"")
    index_file<-apply((cbind(index_file,xml_to_add)),2,as.character)
    write.csv(index_file,file=paste("out/data/",project_folder,"index_",year,r,"_plus_xml",as.character(guardrail),".csv",sep=""),row.names=F)
    #A little cleanup before the multiple responses section
    index_file<-index_file_orig
}
stem<-paste0("out/data/",project_folder,"index_",year,r,"_plus_xml")
datafile <- read.csv(paste0(stem,as.character(increment),".csv"), header=T, sep=",", as.is=T)
for (i in seq(increment*2,max,increment)){
  print(i)
  append <- read.csv(paste0(stem,as.character(i),".csv"), header=T, sep=",")
  datafile<-rbind(datafile,append)
  file.remove(paste0(stem,as.character(increment),".csv"))
}

append <- read.csv(paste0(stem,as.character(max),".csv"), header=T, sep=",")
datafile<-rbind(datafile,append)
datafile$OBJECT_ID<-gsub("\"","",datafile$OBJECT_ID) #to fix the fact that read.csv can't read long numbers

write.csv(datafile,paste0(stem,'.csv'))

rm(index_file_orig)

increment=1000
min=0#nrow(index_file)-nrow(index_file)%%increment-increment
max=2000#nrow(index_file)-nrow(index_file)%%increment
for (guardrail in seq(increment+min, max, increment)) {
    mod<-0
    if (guardrail==max){
      mod=nrow(index_file)%%increment
    }
    print((guardrail-increment+1):(guardrail+mod))
    #Multiple responses per variable per return: Recipient Tables
    start=Sys.time()
    print(start)
    prefixes<-unique(xml_vars_recipient$prefix)
    xml_vars_recipient_orig<-xml_vars_recipient
    for (prefix in prefixes){
        short_prefix<-sub(".*/", "", prefix)
        xml_vars_recipient<-xml_vars_recipient[xml_vars_recipient$prefix==prefix,]
        recipients<-foreach(i=(guardrail-increment+1):(guardrail+mod),.combine=rbind,.inorder = FALSE) %dopar% {
            library(xml2)

            if (i%%1000==0) print(paste0(i,"/",guardrail," records complete"))

            obj_id<-index_file$OBJECT_ID[i]

            amazonurl<-paste("https://s3.amazonaws.com/irs-form-990/",obj_id,"_public.xml", sep="")
            xdata<-obj_id

            doc<-download_tax_return(amazonurl,xdata)
            recipients_count=as.numeric(length(as_list(xml_find_all(doc,prefix)))[1])

            if (recipients_count==0){
                rm(doc)
                detach("package:xml2",unload=T)
                to_add<-data.frame()
            }
            if (recipients_count>0){
                paths<-apply(data.frame(c(1:recipients_count)),1,
                function(j){
                    apply(as.data.frame(xml_vars_recipient$path),1,function(x){paste(prefix,"[",j,"]/",x,sep="")})
                }
                )
                to_add<-data.frame(t(sapply(paths[,1],get_item_contents,doc=doc)))
                colnames(to_add)<-xml_vars_recipient$r_var
                to_add$Recipient_Num<-1
                if (recipients_count>1){
                    for (k in 2:recipients_count){
                        new_row<-data.frame(t(sapply(paths[,k],get_item_contents,doc=doc)))
                        colnames(new_row)<-xml_vars_recipient$r_var
                        new_row$Recipient_Num<-k
                        to_add<-rbind(to_add,new_row)
                    }
                    rm(new_row)
                }
                to_add$OBJECT_ID<-obj_id
                rm(paths)
                rm(doc)
                rm(k)
                detach("package:xml2",unload=T)

            }
            to_add
        }
        recipients$OBJECT_ID<-paste0("\"",recipients$OBJECT_ID,"\"")
        write.csv(as.data.frame(recipients),file=paste("out/data/",project_folder,short_prefix,"_",year,r,as.character(guardrail),".csv",sep=""))
        xml_vars_recipient<-xml_vars_recipient_orig
        print(Sys.time()-start)
    }
}

stem<-paste0("out/data/",project_folder,short_prefix,"_",year,r)
datafile <- read.csv(paste0(stem,as.character(increment),".csv"), header=T, sep=",", as.is=T)
for (i in seq(increment*2,max,increment)){
  print(i)
  append <- read.csv(paste0(stem,as.character(i),".csv"), header=T, sep=",")
  datafile<-rbind(datafile,append)
  file.remove(paste0(stem,as.character(increment),".csv"))
}

append <- read.csv(paste0(stem,as.character(max),".csv"), header=T, sep=",")
datafile<-rbind(datafile,append)
datafile$OBJECT_ID<-gsub("\"","",datafile$OBJECT_ID) #to fix the fact that read.csv can't read long numbers

write.csv(datafile,paste0(stem,'.csv'))
