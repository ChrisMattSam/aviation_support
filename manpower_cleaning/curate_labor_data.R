rm(list = ls())
gc()

library(data.table)
library(readxl)
library(plyr)
library(tictoc)
if('csampah.ida' %in% rownames(installed.packages())) { library(csampah.ida)
  } else( source('//path/Analysis/Functions/csampah.ida/R/basic_functions.R') )

source('//path/Analysis/Functions/utility_functions.R')

output_path = '//path/Analysis/Data Cleaning/analyze_avcrad_manpower_data/Output/Labor/'

dp = '//path/Data/Original/20190916 TASMG traveling team data/'

tic('Reading in all datasets')
tasmgs <- list('ca' = read.all.excel(the_path = paste0(dp,'CA/'), the.tab = 'labor',combine = TRUE),
                'ct' = read.all.excel(the_path = paste0(dp,'CT/'), the.tab = 'Labor',combine = TRUE),
                'mo' = read.all.excel(the_path = paste0(dp,'MO/'), the.tab = 'DATA', combine = TRUE),
                'ms' = read.all.excel(the_path = paste0(dp,'MS/'), the.tab = 'Query', combine = TRUE))
toc()
lapply(tasmgs, function(x){dim(x)})

#fix the weird name format for mo file
nms <- names(tasmgs[['mo']])
tasmgs[['mo']] <- setnames(tasmgs[['mo']], nms,gsub('\r\n','_', nms))

#add field telling me what file the record came from
for(i in 1:length(tasmgs)){tasmgs[[i]][, tasmg := tolower(names(tasmgs)[i])]}

tic('Turn Julian date to normal date, then take data inventory')
for (t in names(tasmgs)) {
  #fix the julian dates to normal dates b4 writing data inventory
  tasmgs[[t]] <- fix.dates(tasmgs[[t]])
  fwrite(DataInventory(tasmgs[[t]]), file = paste0(output_path,'/Data Inventory/',toupper(t),'_labor_data.csv'))
}
toc()

#create new features directly from former features; standardize names as necessary ####
#get all the possible names, then map them to the new column to be created
nms <- c()
for(t in names(tasmgs)){nms <- unique(c(nms, names(tasmgs[[t]]))) }
name.map <- list('employee_id' = nms[grepl('BADGE|ID', nms)],
                 'trans_date_julian' = nms[grepl('TRANS_DATE',nms)],
                 'time_out' = nms[grepl('TIME_OUT', nms)],
                 'time_in' = nms[grepl('TIME_IN', nms)],
                 'won_major' = c(nms[grepl('WON_MAJOR',nms)],'WON'),
                 'work_center' = nms[grepl('WC_WRK|WORK_CENTER',nms)],
                 'ldc' = c('LDC', 'LDC_21'),
                 'op_number' = nms[grepl('OP_NO|OP_CODE',nms)],
                 'man_hours' = nms[grepl('MAN_HOURS',nms)],
                 'hourly_rate' = nms[grepl('EMP_HR_RATE|HOURLY_RATE',nms)],
                 'employment_category' = nms[grepl('CAT_EMP', nms)],
                 'work_code' = nms[grepl('WORK_CODE', nms)],
                 'model' = 'AC',
                 'trans_date' = nms[grepl('trans_date',nms)],
                 'op_category' = c('OP-Cat', 'OP_Cat'))


tic('create new features from existing features, then combine the datasets')
tasmgs2 <- lapply(tasmgs, function(x){add.mapped.features(x, name.map)})
dt.labor <- rbindlist(tasmgs2, fill = TRUE)
fwrite(DataInventory(dt.labor), file = paste0(output_path,'Data Inventory/Combined datasets.csv'))
#filter on fields created by mindy or me
nms <- names(dt.labor)
fwrite(DataInventory(dt.labor[,nms[!(toupper(nms) == nms)], with = FALSE]),
       file = paste0(output_path,'Data Inventory/Combined datasets subsetted on new columns.csv'))
toc()

# data checks ####
dt.labor[!is.na(EIC) & is.na(model),.N] #check missing model types, given an EIC

#check missing EIC
dt.labor[is.na(EIC),.N]
dt.labor[, eic.exist := EIC]
dt.labor[!is.na(EIC), eic.exist := 'not missing']
plyr::count(dt.labor[,c('tasmg', 'eic.exist')])

head(plyr::count(dt.labor[,c('tasmg','OP_lookup', 'eic.exist')]))
t <- plyr::count(dt.labor[!is.na(model), c('OP_lookup')])
t[order(-t$freq),]


#check time_in and time_out
dt.labor[, minutes_spent := as.integer(time_out)-as.integer(time_in)]

for(t in unique(dt.labor$tasmg)) {
  t <- plyr::count(dt.labor[tasmg == t,c('tasmg','time_in')])
  print(head(t[order(-t$freq),],30)) #get the top thirty start times
}
stopifnot(dt.labor[time_out < time_in,.N] == 0)

