rm(list = ls())
gc()

library(data.table)
library(readxl)
library(plyr)
library(tictoc)
library(lubridate)
library(reshape2)

if('csampah.ida' %in% rownames(installed.packages())) {library(csampah.ida)
  } else( sapply(list.files('//path/Analysis/Functions/csampah.ida/R/', full.names = T),
               source))

output_path = '//path/Analysis/Data Cleaning/analyze_avcrad_manpower_data/Output/'

s <- '//path/Analysis/Data Cleaning/analyze_avcrad_manpower_data/Output/'
setwd(s)
dt.labor <- remove_sparse_columns(readRDS(paste0(s,'Labor/labor_data.rds')))
dt.work.order <- remove_sparse_columns(readRDS(paste0(s,'Work Order/workorder_data.rds')))

#compare uniques by WON and tasmg ####
count.uniques(dt.work.order, c('won_major','tasmg'))
cols_ <- c('employee_id','won_major','tasmg', 'time_in', 'time_out')
for(c in cols_) { if(dt.labor[is.na(get(c)),.N] > 0) {print(paste('Count of records missing a value for ',c,':',
                                                                  dt.labor[is.na(get(c)),.N]))}}
#merge datasets & create fields ####
tic('Merge the datasets, remove sparse columns, count and remove duplicates:')
dt <- remove_sparse_columns(data = merge(dt.labor,
                                              dt.work.order,
                                              by = c('tasmg','won_major'),
                                              all.x = TRUE))
if(all(dim(dt) != dim(unique(dt)))) { dt <- unique(dt)}
toc()

dt[, eic := EIC.x]
tic('Add aircraft family name (e.g. Chinook, Blackhawk, etc.):')
dt[, family_name := aircraft(AC)]
toc()

#create report date
dt[, report_date := trans_date]
dt[day(trans_date) > 15 , report_date:= trans_date %m+% months(1)]
mday(dt$report_date) <- 15

#find missing date ranges
missing_months <- function(date.name, start.date = NA, end.date = NA, data) {
  'I need to edit this: return a table format, features = "month", "Value or No Value"'
  #get the min date, max date, and return the months between these dates where no records exist
  the.dates <- data[ year(get(date.name)) < 2019, get(date.name)]
  if(!is.na(start.date)) {
    the.dates <- data[ year(get(date.name)) < 2019 & get(date.name) >= start.date,get(date.name)]}
  if(!is.na(end.date)) {
    the.dates <- data[ year(get(date.name)) < 2019 & get(date.name) <= end.date]}

  data.months <- format(as.Date( the.dates),"%Y-%m")
  date.range <- as.Date(seq.Date(min(the.dates, na.rm = TRUE),max(the.dates, na.rm = TRUE),'month'))

  year.month <- format(date.range, "%Y-%m")
  print(paste('For date value:', date.name))
  print("The months with no dates are as follows ('yyyy-mm' format):")
  print(year.month[!(year.month %in% data.months)])
  print('')}
for(d in sort(names(dt)[grepl('date', names(dt)) & !grepl('julian', names(dt))])){missing_months(d,data = dt)}

# map facility onto the data with uic as the key ####
mapper <- fread('//path/Data/Processed/Master flight facility ID dictionary/master_flight_facility_dict.csv')
s <- 'UIC_OWN1'
temp <- setnames(mapper, names(mapper), paste0(names(mapper), gsub('UIC','',s)))
dt <- merge(dt, temp, by.x = s, by.y = s, all.x = TRUE)
temp <- setnames(temp, names(temp),gsub('_OWN1|_SPT1','',names(temp)))

s <- 'UIC_SPT1'
temp <- setnames(mapper, names(mapper), paste0(names(mapper), gsub('UIC','',s)))
dt <- merge(dt, temp, by.x = s, by.y = s, all.x = TRUE)

print('Out of the available UICs, what percentage of each failed to merge to a facility value:')
print('For UIC_SPT1:')
dt[!is.na(UIC_SPT1) & is.na(facility_id_SPT1),.N]/dt[!is.na(UIC_OWN1),.N]
print('For UIC_OWN1:')
dt[!is.na(UIC_OWN1) & is.na(facility_id_OWN1),.N]/dt[!is.na(UIC_OWN1),.N]

#fill-in missing facility with readiness facility, with uic as the key ####
t <- setnames(readRDS(paste0('//path/Data/Processed/',
                              'Master aircraft readiness data from Devon Hall unduped/Processed_Aircraft_Readiness_FY2007_to_201909.rds')),
                      'TASMG', 'tasmg')

t <- unique(t[!grepl(' |BEST|DEP',uic) & !is.na(family_name) & nchar(uic) > 3 & nchar(facility_id) < 4 & !is.na(STATE),
        .(uic,facility_id,report_date,family_name,STATE, tasmg)])

setorderv(t, c('uic', 'facility_id', 'report_date'))

#UIC_SPT1 first
summ.count <- plyr::count(dt[UIC_SPT1 %in% unique(t[!is.na(facility_id)]$uic) & is.na(facility_id_SPT1)],
                          'UIC_SPT1')
uic.without.facility <- c()
for( the.uic in summ.count$UIC_SPT1) {
  print(paste('For UIC', the.uic,', the facility ID in question is:'))
  temp <- unique(dt[UIC_SPT1 == the.uic, facility_id_SPT1])
  print(temp)
  if(is.na(temp)) {uic.without.facility <- c(uic.without.facility,the.uic)}}

print('Count missing facilities, impute facility for specific facilities, recount missing facilities')
dt[!is.na(UIC_SPT1) & !is.na(UIC_OWN1) & is.na(facility_id_SPT1),.N] 
for(na.val in uic.without.facility) {print(paste0(na.val,':',unique(dt[UIC_SPT1 == na.val, facility_id_SPT1])))}
for(na.val in uic.without.facility) {dt[UIC_SPT1 == na.val, facility_id_SPT1 :=unique(t[uic == na.val, facility_id])]}
for(na.val in uic.without.facility) {print(paste0(na.val,':',unique(dt[UIC_SPT1 == na.val, facility_id_SPT1])))}
dt[!is.na(UIC_SPT1) & !is.na(UIC_OWN1) & is.na(facility_id_SPT1),.N]


#repeat with UIC_OWN1
#after the first merge, how many UIC_OWN1 dont have a facility id
summ.count <- plyr::count(dt[UIC_OWN1 %in% unique(t[!is.na(facility_id)]$uic) & is.na(facility_id_OWN1), 'UIC_OWN1'])
uic.without.facility <- c()
for( the.uic in summ.count$UIC_OWN1) {
  if(is.na(unique(dt[UIC_OWN1 == the.uic, facility_id_OWN1]))) {uic.without.facility <- c(uic.without.facility,the.uic)}}

t2 <- as.data.table(plyr::count(t, c('uic', 'facility_id')))
t2[, max.freq := max(freq), by = 'uic'] #per uic, get the facility that shows up the most

print('Number of UIC_OWN1 records that do not have a facility ID:')
print(paste('before merge:',dt[!is.na(UIC_SPT1) & !is.na(UIC_OWN1) & is.na(facility_id_OWN1),.N]))
dt <- left_merge(dt,
                 setnames(t2[max.freq == freq, .(uic,facility_id)], 'facility_id', 'readiness_OWN1_facility'),
                 list('left' = 'UIC_OWN1','right' = 'uic'))
dt[is.na(facility_id_OWN1) & !is.na(readiness_OWN1_facility), facility_id_OWN1 := readiness_OWN1_facility]
print(paste('after merge:',dt[!is.na(UIC_SPT1) & !is.na(UIC_OWN1) & is.na(facility_id_OWN1),.N]))

# look specifically into UICs ####
#general number of uics
b <- rep(dt[!is.na(UIC_SPT1) & !is.na(UIC_OWN1),.N],2) # total num of records with no missing UIC
a <- c(dt[!is.na(facility_id_OWN1),.N],dt[!is.na(facility_id_SPT1),.N])
print('(% UIC_OWN1, % UIC_SPT1) that have a facility ID:')
round(a/b,3)

#unique number of uics
b2 <- c(length(unique(dt[!is.na(UIC_OWN1)]$UIC_OWN1)),
        length(unique(dt[!is.na(UIC_SPT1)]$UIC_SPT1)))

a2 <- c(length(unique(dt[!is.na(facility_id_OWN1)]$UIC_OWN1)),
  length(unique(dt[!is.na(facility_id_SPT1)]$UIC_SPT1)))
print('Repeat above, but on the set of unique UICs')
print('(% UIC_OWN1, % UIC_SPT1) that have a facility ID:')
round(a2/b2,3)

print('Out of the uniques, what kind of facilities are we dealing with:')
unique(dt[!is.na(facility_id_SPT1), c('tasmg',names(dt)[grepl('SPT1', names(dt))]), with = FALSE])
unique(dt[!is.na(facility_id_OWN1), c('tasmg',names(dt)[grepl('OWN1', names(dt))]), with = FALSE])

# counts for Nate and Mike ####
p <- '//path/Analysis/Data Cleaning/analyze_avcrad_manpower_data/Ancillary/'
key <- as.data.table(setnames(fread(paste0(p,'employee_code_key.csv')), c('code', 'job_title')))

temp.table <- merge(dt, setnames(key, 'job_title', 'CAT_EMP_21_job'),
                    by.x = c('CAT_EMP_21'), by.y = 'code', all.x = TRUE)

temp.table <- merge(temp.table, setnames(key, 'CAT_EMP_21_job', 'CAT_EMP_job'), 
                    by.x = c('CAT_EMP'), by.y = 'code', all.x = TRUE)

temp.table[, job := CAT_EMP_21_job]
temp.table[is.na(job), job := CAT_EMP_job]
temp.table[work_center == 'AM4',.N]
plyr::count(temp.table[work_center == 'AM4']$job)
plyr::count(temp.table[work_center != 'AM4']$job)

# obtain counts for Mike ####
#count of unique WON's that didn't match to any facility
no_match <- unique(dt[is.na(facility_id_SPT1) & is.na(facility_id_OWN1), won_major])
match <- unique(dt[!is.na(facility_id_SPT1) & !is.na(facility_id_OWN1), won_major])
length(unique(dt[won_major %in% no_match & !(won_major %in% match), won_major]))

#count of unique facilities that didn't match to a WON
no_match <- unique(dt[is.na(won_major), facility_id_SPT1])
match <- unique(dt[!is.na(won_major) , facility_id_SPT1])
length(unique(dt[facility_id_SPT1 %in% no_match & !(facility_id_SPT1 %in% match), facility_id_SPT1]))

#aggregate final dataset####
dt[, total_man_hours := sum(man_hours), by = c('facility_id_OWN1', 'report_date','work_center')]
dt[is.na(facility_id_OWN1) | is.na(facility_id_SPT1) | work_center != 'AM4', total_man_hours := NA]

dt.final <- setnames(unique(dt[!is.na(facility_id_OWN1) & !is.na(total_man_hours),
                      .(facility_id_OWN1, report_date, total_man_hours)]),
                     'facility_id_OWN1', 'facility_id')

p <- '//path/Data/Processed/Master travelling teams data/'
for(v in c(T,F)) {cwrite(dt,'travelling_teams_data',paste0(p,'merged'),v)} #the raw, merged, unduped data
for(v in c(T,F)) {cwrite(dt.final,'final_travelling_teams_data',p,v)}



