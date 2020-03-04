rm(list = ls())
gc()

library(data.table)
library(readxl)
library(plyr)
library(tidyr)
library(tictoc)
if('csampah.ida' %in% rownames(installed.packages())) { library(csampah.ida)
} else( source('//readiness_path/Analysis/Functions/csampah.ida/R/basic_functions.R'))

source('//readiness_path/Analysis/Functions/utility_functions.R')

output_path = paste0('//readiness_path/Analysis/Data Cleaning/analyze_avcrad_manpower_data/Output/Work Order/')

dp = paste0('//readiness_path/Data/Original/',
                   '20190916 TASMG traveling team data/')

tic('Reading in all datasets')
tasmgs <- list('ca' = as.data.table(read_excel(list.files(paste0(dp,'CA/'), full.names = TRUE),'WON-EIC')),
               'ct' = read.all.excel(the_path = paste0(dp,'CT/'), the.tab = 'WON',combine = TRUE),
               'mo' = read.all.excel(the_path = paste0(dp,'MO/'), the.tab = 'WON', combine = TRUE),
               'ms' = read.all.excel(the_path = paste0(dp,'MS/'), the.tab = 'Job Order', combine = TRUE))
#create a col in each dataset to tell me which tasmg the data came from
for( i in 1:4) {tasmgs[[i]][, tasmg := names(tasmgs)[i]]}
toc()


#apply artificial col name fixes to make the col names resemble eah other more
# remove 01 or X_01 pattern from the end of field names 
names(tasmgs[['ca']]) <- gsub('_01|_X_01','',names(tasmgs[['ca']]))
names(tasmgs[['ms']]) <- gsub('_01|_X_01','',names(tasmgs[['ms']]))

setnames(tasmgs[['ct']],'ADMIN_JOB_ORDER.WON_MAJOR', 'WON_MAJOR')

'common fields in all datasets'
shared_names <- Reduce(intersect, lapply(tasmgs, function(x){names(x)}))

#backfill col names for state MO to allow data table combination
cols <- lapply(tasmgs, function(x){
  #order cols by the common ones
  c(names(x)[names(x) %in% shared_names], names(x)[!(names(x) %in% shared_names)])
  })
cols[[3]] <- c(cols[[3]], rep('placeholder', max(unlist(lapply(cols, length)))-length(cols[[3]])))

temp <- data.frame(cols[[1]], cols[[2]], cols[[3]], cols[[4]])
names(temp) <- names(tasmgs) # name the cols after its appropriate state
fwrite(temp, file = paste0(output_path,'won_name_fields.csv'))

dt.work.order <- rbindlist(tasmgs, fill = TRUE)
#remove feature 'Field0'; same as WON major when populated
dt.work.order$Field0 <- NULL
fwrite(DataInventory(dt.work.order), file = paste0(output_path,'/Data Inventory/Data Inventory.csv') )
for (t in names(tasmgs)) {
  fwrite(DataInventory(tasmgs[[t]]), file = paste0(output_path,'/Data Inventory/',toupper(t),'_only.csv'))
}

#correct dates
head(dt.work.order[,names(dt.work.order)[grepl('DATE', names(dt.work.order))], with = FALSE])
for( nm in names(dt.work.order)[grepl('DATE', names(dt.work.order))]) {dt.work.order[,eval(nm) := as.numeric(get(nm))]}
dt.work.order <- fix.dates(dt.work.order)

#print dates with their fixed counterparts
nms <- names(dt.work.order)[grepl('DATE', names(dt.work.order))]
head(dt.work.order[,sort(c(nms, tolower(nms))), with = FALSE])

dt.work.order <- dt.work.order[,unique(c('tasmg', names(dt.work.order))), with = FALSE]
setnames(dt.work.order, 'WON_MAJOR', 'won_major')
fwrite(dt.work.order, file = paste0(output_path,'/workorder_data.csv'))
saveRDS(dt.work.order, file = paste0(output_path,'/workorder_data.rds'))
