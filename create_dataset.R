'christopher sampah'


rm(list =ls())
gc()


library(data.table)
library(plyr)
library(dplyr)
library(lubridate)
library(tictoc)
library(ggplot2)

if("csampah.ida" %in% rownames(installed.packages())) {library(csampah.ida)
} else(source('//readiness_path/Analysis/Functions/csampah.ida/R/basic_functions.R'))

output <- '//readiness_path/Analysis/Data Cleaning/create readiness covariates for modeling/Output/'
#local <- 'C:/Users/csampah/Documents/FTS 2b/temp_output/'
dt.original <- readRDS(paste0('//readiness_path/Data/Processed/',
                     'Master aircraft readiness data from Devon Hall unduped/',
                     'Processed_Aircraft_Readiness_FY2007_to_201909.rds'))

# request for mike and initial data filtering ####
ff <- c()
for( field in names(dt.original)) {if( !all(grepl('AVCRAD',unique(dt.original[,get(field)])) == F)) { ff <- c(field,ff)}}
print('fields with "AVCRAD" text string in them:')
print(ff)
dt <- setorderv(dt.original, 'report_date', 1)
for( value in c('1106', '1109', '1107', '1108')) {
  print(value)
  print(min(dt[grepl(value,unit_name) & grepl('AVCRAD',unit_name) , report_date]))
  t.date <- max(dt[grepl(value,unit_name) & grepl('AVCRAD',unit_name) , report_date])

  print(head(unique(dt[grepl(value,unit_name) & report_date >= t.date,
                  .(unit_name,report_date, facility_id)])))}

dt <- dt[report_date > "2010-09-15"]

# 1., 2.,3.,4. cols to write in the final table
dt[pmc > poss_hrs, pmc := poss_hrs]
dt[, pmc_perc := pmc/poss_hrs]
name_change <- list('old' = c('sq_feet','facility_age','quality'),
                    'new' = c('facility_sq_feet','facility_age','facility_quality'))
setnames(dt, name_change$old, name_change$new)
ic <- name_change$new
ic <- c(ic, 'hours_flown','serial_number','report_date','nmc_perc','facility_id','facility_uic','uic', 'pmc_perc')
fixed_wing <- c('Huron','Citation V', 'Sherpa')
 
# data requests ####
#mike 1
length(unique(dt[year(report_date) >= 2017 & facility_type == 'AASF', facility_id]))
length(unique(dt[year(report_date) >= 2017 & facility_type != 'TASMG' & facility_type !=  'AASF',
                 facility_id]))
dt[, val := max(report_date), by = facility_id]
p <- '//readiness_path/Analysis/Data Cleaning/create readiness covariates for modeling/Output/test.csv'
fwrite(unique(dt[year(val) >= 2017,c('facility_id','facility_type','val')]),file = p)

#mike 2 10/11 request
table1 <- setnames(as.data.table(plyr::count(dt, c('facility_type'))), 'freq', 'count')
table1[, pct := round(count/sum(table1$count),3)]

#mike 3 10/17 request
'count of unique AASF facilities in the last month of reliable readiness data'
length(unique(dt[report_date == max(dt$report_date) & !is.na(facility_id) & facility_type == 'AASF',
                 facility_id]))
table1 <- setnames(as.data.table(plyr::count(dt[report_date > '2017-09-15' &
                                                  !(STATUS %in% c('deployed','mobilized'))],
                                             c('facility_type'))), 'freq', 'count')
table1[, pct := round(count/sum(table1$count),3)]


# address one-time fixes####
#UH-60V issue
dt[model == 'UH-60V', model := 'UH-60M']
#TN1 issue: for report date 2015-10-15, 242 aircraft at facility TN1; this is too much
these.sn <- dt[facility_id == 'TN1' & report_date == '2015-10-15', serial_number]
this.data <- dcast(dt[serial_number %in% these.sn & year(report_date) == 2015], facility_id ~ report_date,
                   fun.aggregate = length, value.var = 'serial_number')
fwrite(this.data, file = paste0(output,'TN1 issue-pre fix.csv'))
for( sn in these.sn) {
  facil.1 <- dt[serial_number == sn & report_date == '2015-09-15',facility_id]
  facil.2 <- dt[serial_number == sn & report_date == '2015-11-15',facility_id]
  if(length(facil.1)==0) {facil.1 = 'No facil'}
  if(length(facil.2) ==0) {facil.2 = 'No facil'}
  
  if(facil.1 == facil.2) {
    dt[serial_number == sn & report_date == '2015-10-15',facility_id := facil.1]
  }
  else if(facil.1 != facil.2) {
    dt[serial_number == sn & report_date == '2015-10-15',facility_id := 'drop']
  }
}
this.data <- dcast(dt[serial_number %in% these.sn & year(report_date) == 2015], facility_id ~ report_date,
                   fun.aggregate = length, value.var = 'serial_number')
fwrite(this.data, file = paste0(output,'TN1 issue-post fix.csv'))
dim(dt)
setkey(dt, facility_id)
dt <- dt[!c('drop')]
dim(dt)

# perform reset check & add fields for MCD ####
diff_days <- function(day1, day2) {
  day1
  d1 <- as.Date(as.character(day1), format="%Y-%m-%d")
  d2 <- as.Date(as.character(day2), format="%Y-%m-%d")
  abs(as.numeric(d1-d2))
}

days_of_month <- function(this_report_date) {
  d1 <- as.Date(as.character(this_report_date), format="%Y-%m-%d")
  d2 <- d1 %m-% months(1)
  diff_days(d1,d2)
}

reset <- readRDS('//readiness_path/Analysis/Data Cleaning/curate reset data/Output/reset_intervals.rds')
reset <- reset[start_report_date >= "2010-09-15"]

for(i in 1:nrow(reset)) {
  
  start_report_date <- reset[i, start_report_date]
  finish_report_date <- reset[i, next_MCD_report_date]
  start_date <- reset[i, start_date]
  finish_date <- reset[i, MCD_dt]
  
  this.serial_number <- reset[i,serial_number] 

  #first if condition: reset starts within 30/31 days (exclusive) of report_date
  dt[serial_number == this.serial_number & report_date == start_report_date,
     reset_pct := diff_days(report_date,start_date)/days_of_month(report_date)]
  
  #second if condition: reset ends within 30/31 days (exclusive) of report_date
  dt[serial_number == this.serial_number & report_date == finish_report_date,
     reset_pct := diff_days(report_date,finish_date)/days_of_month(report_date)]
  
  #third if condition: report_date is within the start and end dates
  dt[serial_number == this.serial_number & report_date < finish_report_date & report_date > start_report_date,
     reset_pct := 1]}

dt[, reset := FALSE]
dt[reset_pct > .5, reset := TRUE]
a <- nrow(dt)
print(paste0('Number of dropped records: ',toString(dt[reset ==T,.N])))
dt <- dt[reset==F]
print(paste0('Number of records before, then after reset removal, respectively: ', toString(a),',',toString(dt[,.N])))

dt <- merge(readRDS(paste0('//readiness_path/',
                                         'Analysis/Data Cleaning/curate reset data/Output/time_since_MCD.rds')),
            dt, by = c('serial_number', 'report_date'), all.y = T)
ic <- c(ic, 'reset', 'reset_pct', 'next_MCD_date','previous_MCD_date','days_until_next_MCD','days_since_last_MCD')

# depot maintenance flagging, creation of new var depot_weight, and modifying of nmc_perc ####
dt[, depot_drop := F]
dt[facility_type == 'TASMG', depot_drop := T]
# if uic is a known tasmg uic, drop the record
tasmg_uics <- unique(dt[facility_type == 'TASMG', facility_uic])
dt[uic %in% tasmg_uics, depot_drop := T]
# an 'N' assignment code and either 1) depot_hours > poss_hours or 2) depot% > 1
dt[, assign_is_n := F]
dt[grepl('N|n', assign_func_code) & !grepl('C', assign_func_code), assign_is_n := T]
dt[(DEPOT > poss_hrs | `DEPOT %` >= 1 | STATUS == 'depot') & assign_is_n == T, depot_drop := T]
a <- dim(dt)[1]
dt <- dt[depot_drop == F]
print(paste('Number of dropped records:', a - dim(dt)[1]))

# creation of new var "weight" and modify nmc_perc accordingly
dt[DEPOT > 0 & DEPOT < poss_hrs & assign_is_n == T, nmc_perc := 0]
dt[`DEPOT %` > 0 & `DEPOT %` < 1 & assign_is_n == T, nmc_perc := 0]
dt[DEPOT > 0 & DEPOT < poss_hrs & assign_is_n == T, depot_weight := max(0, min(1 - (DEPOT/poss_hrs),1))]
dt[`DEPOT %` > 0 & `DEPOT %` < 1 & assign_is_n == T, depot_weight := max(0, min(1 - (DEPOT/poss_hrs),1))]

ic <- c(ic, c('depot_weight'))

# flag deployed or mobilized records from the dataset for ALL FY's ####
dt[, deployed_helicopter := F]
dt[(STATUS %in% c('mobilized', 'mob','deployed')) | (assign_func_code == 'DEP'), deployed_helicopter := T]
ic <- c(ic,'deployed_helicopter')

# 5., 6. binary flag for deployment in 1, 3, 6, or 12 months ####
'With STATUS == "depot", dist. of facility types'
dt[STATUS == 'depot', .N,facility_type]
dt <- dt[facility_type == 'AASF']
dt <- n.month.value(dt,'STATUS',1, new.col.name = 'STATUS_next_month')

for(n in seq(2,12)) {dt <- n.month.value(dt,'STATUS',n, print.flag = FALSE,
                                         new.col.name = paste0('STATUS_in_',toString(n),'_months'))}

vals <- c('deployed','mobilized', 'mob')

dt[STATUS_next_month %in% vals | STATUS %in% vals, aircraft_dep_in_1_month_or_less := T]

dt[  (STATUS_in_3_months %in% vals) |
     (STATUS_in_2_months %in% vals) |
     (STATUS_next_month %in% vals), aircraft_dep_in_3_months_or_less := T]

dt[  (STATUS_in_6_months %in% vals) |
     (STATUS_in_5_months %in% vals) |
     (STATUS_in_4_months %in% vals) |
       aircraft_dep_in_3_months_or_less == T,
     aircraft_dep_in_6_months_or_less := T]

dt[  (STATUS_in_12_months %in% vals) |
     (STATUS_in_11_months %in% vals) |
     (STATUS_in_10_months %in% vals) |
     (STATUS_in_9_months %in% vals) |
     (STATUS_in_8_months %in% vals) |
     (STATUS_in_7_months %in% vals) |
       aircraft_dep_in_6_months_or_less == T,
     aircraft_dep_in_12_months_or_less := T]

for(vars in names(dt)[grepl('dep_in', names(dt))]) {dt[is.na(get(vars)), eval(vars) := F]}

ic <- c(ic, names(dt)[grepl('dep_in', names(dt))])

# research on poss_hrs anomalies ####
dt$original_poss_hrs <- dt$poss_hrs
# if poss hrs is zero, check for double reporting in that same report date
dt.temp <- dt[poss_hrs ==0, c('serial_number', 'report_date')]
for ( i in 1:nrow(dt.temp)) {
  if(dt.temp[serial_number == dt.temp[[i,1]] & report_date == dt.temp[[i,2]], .N ] >1) {
    print(paste('Check serial_number',dt.temp[[i,1]],'for month',dt.temp[[i,2]]))}}

t <- plyr::count(dt[poss_hrs ==0 & year(report_date)==2018]$STATUS)
t[order(t$freq),]

dim(dt)
dt <- dt[ poss_hrs != 0]
dt[,drop_me := F]
dt[(report_date >= '2017-10-15') & (STATUS %in% c('reset','crash','KFOR-KOSOVO')), drop_me := T ]
dt <- dt[drop_me == F]
dt[, drop_me := NULL]
dim(dt)

saveRDS(dt,file = paste0('//readiness_path/','Analysis/Data Cleaning/create readiness covariates for modeling/Output/',
                                         '2020_01_14_hours_research/readiness_covariates.rds'))

#if poss hrs is greater than 744 (31 days x 24 hours)
head(dt[poss_hrs > 744, c('serial_number', 'report_date', 'poss_hrs','nmc_perc')])
dt[, date.b4 := report_date - months(1)]
dt[,poss_hrs.new := 24*as.numeric(difftime(report_date,date.b4), units = 'days')]
dt[, poss_hrs.half := poss_hrs*.5]
head(dt[poss_hrs > 744, c('serial_number', 'report_date','date.b4', 'poss_hrs','poss_hrs.half','poss_hrs.new','nmc_perc')])
dt[poss_hrs > 744, poss_hrs := poss_hrs.new]
#if poss hrs less than 28 days X 24 hours, assign to my new poss_hrs
dt[poss_hrs < (28*24), poss_hrs := poss_hrs.new]

#recalculate nmc perc in 2017-06-15
dt[poss_hrs != original_poss_hrs, nmc_perc := NMC/poss_hrs]

# create graphics for mike ####
num_bins <- 50
#plot 1
ggplot(dt[!is.na(nmc_perc) & nmc_perc <=1], aes(x=nmc_perc)) +
  geom_histogram(aes(y = ..count../sum(..count..)),
                 color = 'blue',
                 fill = 'yellow4',
                 bins = num_bins) +
  labs(x = 'NMC %', y = 'proportion')
ggsave(filename = paste0(output,'nmc_perc_distribution.png'))

# create hours flown of previous month ####
new_cols <- c('prev_month_hours_flown','prev_month_total_airframe_hours')
dt.original <- n.month.value(dt.original, 'hours_flown',-1,new.col.name = new_cols[1])
dt.original <- n.month.value(dt.original, 'total_airframe_hours',-1,new.col.name = new_cols[2])
dt <- merge(dt, dt.original[,c('report_date', 'serial_number', new_cols), with = FALSE],
            by = c('report_date', 'serial_number'), all.x = TRUE)
dt <- three_month_average(dt,'hours_flown')
setnames(dt,'three_month_average_hours_flown', 'hours_flown_3_month_mean')
ic <- c(ic,new_cols,'hours_flown_3_month_mean')

# research errors wrt previous month total airframe hours ####
#find the last month the record existed in the dataset
dt[, last.date.exist := shift(report_date, type = 'lag', n = 1), by = serial_number]
dt[, flag := 'address_issue']
dt[!is.na(prev_month_total_airframe_hours), flag := 'ignore']
dt[, date_prior := report_date %m-% months(1)]
dt[last.date.exist != date_prior, flag := 'broken_panel']
dt[is.na(last.date.exist), flag := 'first_appearance_in_dataset']

# try to impute for missing facility, then re-impute attributes of facilities ####
# if last month's state matches this month's state, make this month's missing facility last month's facility
h0 <- dt[is.na(facility_id), .N]
dt <- n.month.value(dt, 'STATE',-1, new.col.name = 'prior_state')
dt <- n.month.value(dt, 'facility_id', -1, new.col.name = 'prior_facility')
dt[is.na(facility_id) & STATE == prior_state & !is.na(prior_facility), facility_id := prior_facility]

h1 <- dt[is.na(facility_id),.N]
for(sn in unique(dt[is.na(facility_id), serial_number])) { # obtain serial numbers with missing facility
  dts <- dt[serial_number == sn & is.na(facility_id), report_date] #obtain the dates where facility is missing
  #print(paste('for serial number',sn))
  for ( i in 1:length(dts)) {
    a <- dt[serial_number == sn & report_date == (dts[i] %m-% months(1)), facility_id]
    b <- dt[serial_number == sn & report_date == (dts[i] %m+% months(1)), facility_id]
    if( length(a) > 0 & length(b) == 0) {
      if(!is.na(a) & nchar(a) > 0 ) {dt[serial_number == sn & report_date == dts[i], facility_id := a]}
      }
    else if(length(b) > 0 & length(a) == 0) {
      if(!is.na(b) & nchar(b) > 0 ){dt[serial_number == sn & report_date == dts[i], facility_id := b]}
    }}}
print(paste('Number of records with missing facility ID before processing:',h0))
print(paste('Number of records with missing facility IDs: after first process =',h1,
            ', after second process =',dt[is.na(facility_id),.N]))

#fill in values for newly imputed facilities
return.issue.facility <- function(field,data = dt) {

  missing.facility.age <- unique(data[is.na(get(field)),facility_id])
  not.missing.facility.age <- unique(data[!is.na(get(field)),facility_id])
  intersect(missing.facility.age, not.missing.facility.age)}
facility.fields <- names(dt.original)[grepl('facility|facility_quality|facility_sq_feet', names(dt.original)) &
                                           !grepl('facility_name|facility_id', names(dt.original)) ]
for(col in facility.fields) {
  print(paste('The field of interest is',col,', with erred facilities:'))
  print(return.issue.facility(col))
}

map2 <- setnames(fread(list.files(paste0('//readiness_path/Data/Processed/',
                                 'Master flight facility ID dictionary/'),full.names = TRUE)[2]),
         c('facility_age', 'sq_feet', 'quality'), c('age_2019.2','facility_sq_feet','facility_quality' ))
#impute facility_age first
dt <- merge(dt, unique(map2[,c('facility_id', 'age_2019.2')]), by  = 'facility_id', all.x = TRUE)
dt[is.na(facility_age), facility_age := age_2019.2-(2019 - year(report_date))]
#impute the other values
vals <- c('facility_sq_feet','facility_quality','facility_type', 'UIC')
setnames(map2, vals,paste0(vals,'2'))
dt <- merge(dt, unique(map2[,c('facility_id',paste0(vals,'2')), with = FALSE]), by = 'facility_id',
            all.x = TRUE)
dt[is.na(facility_uic), facility_uic := UIC2]
dt[is.na(facility_type), facility_type := facility_type2]
dt[is.na(facility_sq_feet), facility_sq_feet := facility_sq_feet2]
dt[is.na(facility_quality), facility_quality := facility_quality2]

#see if there are issues
for(col in facility.fields) {
  print(paste('The field of interest is',col,', with erred facilities:'))
  l <- length(return.issue.facility(col))
  if(l ==0) {print('No issues')}
  else( print(return.issue.facility(col)))}

#manual fixes for missing facility type, per the check directly above this line
dt[facility_id == 'MO5', facility_type := 'TASMG']
dt[facility_id == 'NM2', facility_type := 'AASF']

# include categorical variables 1) model (e.g. (UH-60C, AH-40K), 2) season and 3) family name (e.g. 'Blackhawk', 'Apache') ####
season.month <- list( 'spring' = as.numeric(3:5), 'summer' = as.numeric(6:8),
                      'fall' = as.numeric(9:11), 'winter' = as.numeric(c(12,1,2)))
for(s in names(season.month)) {dt[month(report_date) %in% season.month[[s]], season := s]}
ic <- c(ic,'model','season','family_name')

#check that the month-season combo in my dataset matches the list I created
sort(unique(dt[season == 'winter', month(report_date)]))
sort(unique(dt[season == 'spring', month(report_date)]))
sort(unique(dt[season == 'summer', month(report_date)]))
sort(unique(dt[season == 'fall', month(report_date)]))

# TASMG regions, impute as necessary ####
tasmg.state.map <- list( 'MS' = c('AL','AR','FL','GA','KY','LA','MS','NC','SC', 'PR','TN'),
                         'CT' = c('CT','DC','DE','MA','MD','ME','NH','NJ','NY','PA','RI','VA','VT','WV'),
                         'CA' = c('AK','AZ','CA','CO','GU','HI','ID','MT','NM','NV','OH','OR','UT','WA','WY'),
                         'MO' = c('IA','IL','IN','KS','MA','MN','MO','MI','ND','NE','OK','SD','TX','WI'))

print(paste('Count of missing TASMG before imputation:', dt[is.na(TASMG),.N]))
for(nm in names(tasmg.state.map)) {
  print(paste('The number of records of missing TASMGs with states corresponding to TASMG region',
              nm,'is:',dt[is.na(TASMG) & STATE %in% tasmg.state.map[[nm]], .N]))
  dt[is.na(TASMG) & STATE %in% tasmg.state.map[[nm]], TASMG := nm]}

print(paste('Count of missing TASMG after imputation with STATE field:', dt[is.na(TASMG),.N]))

for(nm in names(tasmg.state.map)) {dt[is.na(TASMG) & !is.na(facility_id) & substr(facility_id,1,2) %in% tasmg.state.map[[nm]],
                                      TASMG := nm]}

print(paste('Count of missing TASMG after imputation with facility id:', dt[is.na(TASMG),.N]))
dt[, TASMG := tolower(paste0(TASMG,'_tasmg'))]
dt[TASMG == 'na_tasmg', TASMG := 'missing_tasmg']
ic <- c(ic, 'TASMG')

# merge travelling teams data onto the dataset ####
travelling_teams.dt <- readRDS(paste0('//readiness_path/Data/Processed/',
                                      'Master travelling teams data/final_travelling_teams_data.rds'))
dt <- right_merge(travelling_teams.dt, dt,list(c('facility_id', 'report_date')))
dt[, date_in_travelling_teams := FALSE]
dt[report_date %in% travelling_teams.dt$report_date, date_in_travelling_teams := TRUE]

#implement the NA/zero adjustment per time range of values
for(this.tasmg in unique(dt$TASMG)) {
  print(paste0('The tasmg is:', this.tasmg))
  min_date <- min(dt[TASMG == this.tasmg & !is.na(total_man_hours)]$report_date)
  max_date <- max(dt[TASMG == this.tasmg & !is.na(total_man_hours)]$report_date)
  dt[TASMG == this.tasmg & report_date >= min_date & report_date <= max_date & is.na(total_man_hours),
     total_man_hours := 0]
  print(min_date)
  print(max_date)
}

setnames(dt, 'total_man_hours', 'traveling_teams_hours')
ic <- c(ic, 'traveling_teams_hours')

# additions from parts data ####
parts <- fread("//readiness_path/Data/Processed/Master parts data/parts_data.csv")
parts$report_date <- as.Date(parts$report_date)
parts[, total_facility_parts_orders := sum(order_count), by = c('facility_id', 'report_date')]
parts[, total_facility_parts_outstanding_days := sum(outstanding_days), by = c('facility_id', 'report_date')]

dt <- merge(dt, setnames(parts[,1:5], c('family','order_count', 'outstanding_days'),
                         c('family_name','within_family_parts_orders','within_family_parts_outstanding_days')),
             by = c('facility_id', 'report_date', 'family_name'), all.x = T)
dt <- merge(dt,
            unique(parts[, !c('order_count', 'outstanding_days', 'family')]),
            by = c('facility_id', 'report_date'), all.x = T)

parts <- merge(parts, setnames(parts[, .SD[facility_id == facility_id, min(report_date), by = .(facility_id)]],
                               'V1', 'first_facility_part_date'), by = c('facility_id'), all.x = T)

parts <- merge(parts, setnames(parts[, .SD[facility_id == facility_id, max(report_date), by = .(facility_id)]],
                               'V1', 'last_facility_part_date'), by = c('facility_id'), all.x = T)

dt  <- merge(dt, unique(parts[, .(facility_id, first_facility_part_date, last_facility_part_date)]),
             by = c('facility_id'), all.x = T)

dt[(paste(facility_id, report_date) %in% parts[, paste(facility_id, report_date)]) & 
     (is.na(total_facility_parts_orders)) & (report_date >= first_facility_part_date) &
     (report_date <= last_facility_part_date), total_facility_parts_orders := 0]

dt[(paste(facility_id, report_date) %in% parts[, paste(facility_id, report_date)]) & 
     (is.na(total_facility_parts_outstanding_days)) & (report_date >= first_facility_part_date) &
     (report_date <= last_facility_part_date), total_facility_parts_outstanding_days := 0]

dt[(paste(facility_id, report_date) %in% parts[, paste(facility_id, report_date)]) & 
     (is.na(within_family_parts_orders)) & (report_date >= first_facility_part_date) &
     (report_date <= last_facility_part_date), within_family_parts_orders := 0]

dt[(paste(facility_id, report_date) %in% parts[, paste(facility_id, report_date)]) & 
     (is.na(within_family_parts_outstanding_days)) & (report_date >= first_facility_part_date) &
     (report_date <= last_facility_part_date), within_family_parts_outstanding_days := 0]

for(feature in c('total_facility_parts_orders', 'total_facility_parts_outstanding_days',
                 'within_family_parts_orders', 'within_family_parts_outstanding_days')) {
  print(paste('total NA values for', feature,'before imputing:'))
  print(dt[is.na(get(feature)),.N])
  
  dt[(report_date >= as.Date('2009-10-01',format='%Y-%m-%d')) &
       (report_date <= as.Date('2019-09-30',format='%Y-%m-%d')) & is.na(get(feature)) & !(facility_id %in% c('DC1','VI1')), eval(feature) := 0 ]
  print(paste('total NA values for', feature,'after imputing:'))
  print(dt[is.na(get(feature)),.N])
}

ic <- c(ic,c('total_facility_parts_orders','total_facility_parts_outstanding_days',
             'within_family_parts_orders','within_family_parts_outstanding_days'))

# get the count of lakotas per facility-month ####
dt[, facility_month_lakota_count := .SD[deployed_helicopter == F & family_name == 'Lakota',.N], by = .(facility_id, report_date)]
ic <- c(ic,'facility_month_lakota_count')

# 15. create dummy for one or more fixed wing at facility month####
dt[, fixed_wing_existence := .SD[deployed_helicopter == F & family_name %in% fixed_wing, .N > 0], by = .(facility_id, report_date)]
ic <- c(ic,'fixed_wing_existence')

# write out the final file ####
ic <- c(ic, 'facility_type')
include_cols <- intersect(ic, names(dt))

saveRDS(include_cols, '//readiness_path/Analysis/Data Cleaning/create readiness covariates for modeling/Output/','cols.RDS')
dt <- dt[family_name %in% c('Apache', 'Blackhawk', 'Chinook', 'Kiowa')]
saveRDS(dt, '//readiness_path/Analysis/Data Cleaning/create readiness covariates for modeling/Output/dt.RDS')
setorderv(dt, cols = c('serial_number', 'report_date'))
dt <- dt[,..include_cols]

# 16. impute for missing months 10/2010 and 2/2012
d1 <- dt[report_date == '2010-11-15',.(report_date, facility_id, serial_number, uic)]
d2 <- dt[report_date == '2012-01-15',.(report_date, facility_id, serial_number, uic)]
d1[, report_date := report_date %m-% months(1)]
d2[, report_date := report_date %m+% months(1)]

dt <- rbindlist(list(dt,d1,d2), fill = T)

fwrite(dt,file = paste0('//readiness_path/Data/Processed/', 'Tables for merge/readiness_covariates_',format(min(dt$report_date), "%Y%m"),'_',
                                                         format(max(dt$report_date), "%Y%m"),'.csv'))
fwrite(dt,file = paste0(output,'readiness_covariates_',format(min(dt$report_date), "%Y%m"),'_',
                                                         format(max(dt$report_date), "%Y%m"),'.csv'))

