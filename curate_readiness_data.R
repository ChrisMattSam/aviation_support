'author: Christopher Sampah'

rm(list = ls())
gc()

library(data.table)
library(lubridate)
library(stringr)
library(tictoc)
if('csampah.ida' %in% rownames(installed.packages())) {library(csampah.ida)
  } else(source('//readiness_path/Analysis/Functions/csampah.ida/R/basic_functions.R'))

source('//readiness_path/Analysis/Functions/utility_functions.R')
#### paths ####
data_location <- paste0('//readiness_path/Data/',
                        'Processed/Master aircraft readiness data from Devon Hall (raw files as CSVs)/')

output_location <- paste0('//readiness_path/Data/',
                          'Processed/Master aircraft readiness data from Devon Hall unduped/')

metadata_location <- paste0('//readiness_path/',
                            'Analysis/Data Cleaning/Curation of readiness data/Output/')

ancillary_location <- paste0('//readiness_path/',
                             'Analysis/Data Cleaning/Curation of readiness data/Ancillary Data/')

#### analysis ####
'Create 2 empty tables to append the read-in files; there are at least 2 different file formats'
batch_1 <- data.table()
batch_2 <-data.table()

filenames <- list.files(path = data_location, pattern="*.csv")

#temporarily remove the FEB fy19 file due to an error
filenames <- filenames[!grepl('FEB', filenames)]

for (name in filenames) {
  file <-fread(paste0(data_location,name), na.strings = c("")) # read-in one datafile

  'Generate the initial metadata file'
  #metadata file function courtesy of Nate Latshaw (nlatshaw@ida.org)
  fname <- paste0('Raw DataInventory ', name) # name the metadata file
  fwrite(DataInventory(file),paste0(metadata_location,fname)) #generate the metadata file
  a <- nrow(file)
  b <- ncol(file)

  'Exclude rows that have NA or blanks for all columns'
  file[, tmp:=apply(file, 1, function(x) sum(is.na(x) | x ==""))]
  cc <- file[tmp == ncol(file), .N]
  file <- file[tmp != ncol(file)]
  file[, tmp := NULL]

  cat(paste0(name, ': ',a,' rows, ', b, ' columns\n','Number of completely blank rows to be dropped: ', cc, '\n'))

  'There are 2 file formats, differing by # of columns'
  'Use this criterion to allocate the read-in file to the correct format'
  n <- 0
  if (ncol(file) == 67) {
    n <- file[`Serial Number` == "" | is.na(`Serial Number`), .N] # count rows with missing serial number
    file <- file[`Serial Number` != "" & !is.na(`Serial Number`)] # drop rows with missing serial number
    batch_1 <- rbind(batch_1, file, fill = FALSE)} # add this file to the larger 67-column file

  else if (ncol(file) == 44) {
    n <- file[`TAIL#` == "" | is.na(`TAIL#`), .N] # count rows with missing TAIL#
    file <- file[`TAIL#` != "" & !is.na(`TAIL#`)] # drop rows with missing TAIL#
    batch_2 <- rbind(batch_2, file, fill = FALSE)} # add this file to the larger 44-column file

  else {break} #if the files break either of the 2 formats above, stop the loop
  cat(paste0('Number of rows to be dropped because of missing serial number: ', n, '\n\n'))}

fwrite(DataInventory(batch_1),paste0(metadata_location,'Raw DataInventory 2007 to 2010.csv'))
fwrite(DataInventory(batch_2),paste0(metadata_location,'Raw DataInventory 2011 to ', batch_2[,format(max(as.Date(`Report Date`, format="%m/%d/%Y")),"%Y%m")], '.csv'))

setnames(batch_1, colnames(batch_1), toupper(colnames(batch_1)))
setnames(batch_2, colnames(batch_2), toupper(colnames(batch_2)))

'Between the batches, which column names match exactly'
intersect(colnames(batch_1), colnames(batch_2))

'Check for duplicate elements and how many'
stopifnot(!duplicated(batch_1))
stopifnot(!duplicated(batch_2))

'Count easily noticeable errors:'
batch_1[`AGE (YEARS) BY ACCEPT DATE`< 0, .N] # check for negative age, which isn't possible
batch_1[grepl("\\?", batch_1$EIC), .N] #alphanumeric field with ? mark in it, count how many of these occurences

batch_2[`HOURS TO PHASE`< 0, .N] # check for negative time, which isn't possible
batch_2[, .SD[ `POSS HRS` == `ADJ POSS HRS`, .N] / .N]

'Merge all data'
combined_batch <- rbind(batch_1, batch_2, fill = TRUE)

'Change name of specific columns'
setnames(combined_batch, c('GAIN/LOSS', 'AGE (YEARS) BY ACCEPT DATE'), c('gain_or_loss', 'age_by_accept_date'))

'Create new columns from combining 2 similarly-valued fields'
'See fxn documentation as needed'
combined_batch[is.na(`GOLD UIC`), `GOLD UIC` := `LIW UIC`]
matcher <- as.data.table(rbind(c('uic', 'UIC','GOLD UIC'),
c('mc', 'MC','MC HRS'),
c('qty_reported', 'QTY REPORTED FOR READINESS','QTY RPT'),
c('assign_func_code', 'ASSIGN/ FUNC CODE', 'ASSIGN/FUNC CODE'),
c('total_airframe_hours', 'TOTAL AIRFRAME HOURS', 'TOTAL AIRFRAME HRS'),
c('report_date','REPORT DATE', 'AS OF DATE'),
c('on_hand', 'ON HAND', 'ON HAND INVENTORY'),
c('unit_name', 'UNIT', 'NAME'),
c('model', 'MODEL', 'MDS')))
#names(matcher) <- c('new_name', 'file_1','file_2')
for( i in 1:dim(matcher)[1]){ 
  combined_batch <- combine_fields(combined_batch,matcher[[i,1]], matcher[[i,2]],matcher[[i,3]])
  }

'For serial number: remove hyphens, ensure all values are of length 7, and ensure there are no letters'
combined_batch[ , `TAIL#` := str_remove(`TAIL#`, "-")]
combined_batch <- combine_fields(combined_batch, 'serial_number', 'SERIAL NUMBER', 'TAIL#')
combined_batch[, serial_number := str_pad(serial_number, 7,'left', '0' )] #this handles leading zeros that are truncated upon read-in
combined_batch <- combined_batch[serial_number != "M822378"] #omit the one serial number that has a letter; report date is not in period of analysis
stopifnot(!grepl("^[A-Za-z]", combined_batch$serial_number)) #this checks for the existence of letters in the field

'Create new columns directly from older ones'
combined_batch[, hours_to_phase := `HOURS TO PHASE`]
combined_batch[, hours_flown := `HOURS FLOWN`]
combined_batch[, facility_name := AASF]
combined_batch[, facility_id := `AASF CODE`]
combined_batch[, state_readiness:= STATE]

'Switch WV2 and WV3 facilities at the behest of project leaders'
combined_batch[, orig.facility_id := facility_id]
combined_batch[orig.facility_id == 'WV2', facility_id :=  'WV3']
combined_batch[orig.facility_id == 'WV3', facility_id := 'WV2']
combined_batch$orig.facility_id <- NULL

'Impute for missing total_airframe_hours'
combined_batch[, new_tot_hours := 0]
tic('Impute total airframe hours for missing values')
j <- 0
all_sn <- unique(combined_batch[!is.na(total_airframe_hours),serial_number])
for (sn in all_sn) {

  max.date <- max(combined_batch[serial_number==sn & !is.na(total_airframe_hours), report_date])
  date.range <- sort(combined_batch[report_date <= max.date & serial_number == sn, report_date])
  fix <- combined_batch[report_date == max.date & serial_number == sn, total_airframe_hours]
  i <- length(date.range)
  while ( i > 1) {
    prev.hours.flown <- sum(combined_batch[report_date <= max.date
                                           & report_date > date.range[i-1]
                                           & serial_number == sn
                                           & !is.na(hours_flown), hours_flown])
    combined_batch[serial_number == sn & report_date == date.range[i-1], new_tot_hours := fix -prev.hours.flown]
    i <- i -1}
  j <- j + 1
  print(paste('Remaining serial numbers to impute total airframe hours for:', (length(all_sn)-j) ))}
combined_batch[new_tot_hours < 0 , new_tot_hours :=0]
combined_batch[is.na(total_airframe_hours) & !is.na(new_tot_hours), total_airframe_hours := new_tot_hours]
setkeyv(combined_batch, cols = c('serial_number','report_date'))
toc()

saveRDS(combined_batch,paste0('//readiness_path/Analysis/Data Cleaning/',
       'Curation of readiness data/Output/Archive/intermediate_dataset.rds'))

'Create facility_type field, add facility_uic and square feet'
dt <- fread(list.files(paste0('//readiness_path/Data/Processed/',
                              'Master flight facility ID dictionary/'),full.names = TRUE)[2])
setnames(dt, c('UIC','facility_age'), c('facility_uic','age_2019') )
dt <- unique(dt[,!c('State','construction_date')])
dt[quality=="", quality := NA]
combined_batch <- merge(combined_batch, dt, by  = 'facility_id', all.x = TRUE)
combined_batch[quality == " " | quality == "", quality := NA]
#change the facility age
combined_batch[, facility_age := age_2019-(2019 - year(report_date))]
combined_batch[is.na(facility_type),.N]
combined_batch[is.na(facility_uic),.N]
combined_batch[grepl("TASMG", facility_name) == TRUE & is.na(facility_type), facility_type := 'TASMG']
combined_batch[grepl("AASF", facility_name) == TRUE & is.na(facility_type), facility_type := 'AASF']
combined_batch[is.na(facility_type),.N]


'Standardize the "model" field'
combined_batch[grepl('\\\\',model), model := gsub('\\\\','',model)] #remove \\ chars in model values
combined_batch[,model := gsub(' ','',model)] #remove spaces
combined_batch[,model := toupper(model)] #all values are caps
all_models <- unique(combined_batch$model)
sseq <- c('0','1','2','3','4','5','6','7','8','9')

#if there's no dash, insert the dash accordingly
dashed_vals <- all_models[!grepl('-',all_models)]
for (i in 1:length(dashed_vals)) {
  val <- dashed_vals[i]
  new_val <- ''
  if (substr(val,2,2) %in% sseq) { new_val <- paste0(substr(val,1,1),'-',substr(val,2,nchar(val)))}
  else if(substr(val,3,3) %in% sseq) {new_val <- paste0(substr(val,1,2),'-', substr(val,3,nchar(val)))}
  combined_batch[model==dashed_vals[i], model := new_val]}
#fix other non-standard issues
fix_dict <- data.table(orig = c('C-23C1', 'C-26EE','AH-64DII'),
                       new=c('C-23C','C-26E','AH-64D'))
f_d <- fix_dict
for (i in 1:nrow(f_d)){combined_batch[model==f_d[i,1], model := f_d[i,2]]}

'Investigate serial numbers with model UH-60'
distinct_serial <- unique(combined_batch[model =='UH-60', serial_number])
uh_60s <- data.table(serial_num = distinct_serial )
for(num in distinct_serial) {
  uh_60s[serial_num==num, num_count := combined_batch[serial_number==num & model == 'UH-60', .N]]
  #print(paste('Serial number:',num))
  its_models <- unique(combined_batch[serial_number==num, model])
  for(m in its_models) {
    #print(paste(',model',m,'the report dates ranged from:'))
    dd <- combined_batch[serial_number==num & model==m, report_date]
    #print(paste(min(dd),'to', max(dd)) )
    #print('')
    }}

#look at other serial numbers that transition b/w UH60L and UH60A
temp <- combined_batch[model=='UH-60A', serial_number] #subset on sn's with UH60A model
temp_dt <- combined_batch[serial_number %in% temp & model=='UH-60L'] #out of the above sn's, pick ones that also have UH60L

for(num in unique(temp_dt[,serial_number])) {
  its_models <- unique(combined_batch[serial_number==num, model])
  #print(paste('Serial number:',num))
  for(m in its_models) {
    if ( !(m %in% c('UH-60A', 'UH-60L')) ) {break}
    #print(paste(',model',m,'the report dates ranged from:'))
    dd <- combined_batch[serial_number==num & model==m, report_date]
    #print(paste(min(dd),'to', max(dd)) )
    #print('')
    }}

#the below serial numbers have UH60 model that can confidently be imputed
for (s_num in c('8423938','8323905')) {combined_batch[serial_number==s_num & model=='UH-60', model :='UH-60A']}
for (s_num in c('8624550','8323900')) {combined_batch[serial_number==s_num & model=='UH-60', model :='UH-60L']}

#ensure ONLY letters, chars, and hyphens populate the model field
for (model in all_models) {
  val <- unlist(strsplit(model,''))

  for (v in val) {
    poss_vals <- c(LETTERS,sseq,'-')
    stopifnot(v %in% poss_vals)}}
'tabulated models field'
tmf <- as.data.frame(table(combined_batch[,c('model')]))
fwrite(tmf, paste0(ancillary_location,'Tabulated Models Field.csv'))

'Create a new "Model Family" field'
for (i in 1:length(all_models)) {
  m <- all_models[i]
  while(substr(m,nchar(m),nchar(m)) %in% LETTERS){m <- substr(m,1,nchar(m)-1)}

  #handle the special case where the last 2 values are a number and a letter, e.g. C-23C1 should be C-23
  if(substr(m,nchar(m),nchar(m)) %in% sseq & substr(m,nchar(m)-1,nchar(m)-1) %in% LETTERS){
    m <- substr(m,1,nchar(m)-2)}
  combined_batch[model==all_models[i], model_family :=m]}
combined_batch[model_family=='EH-60', model_family :='UH-60']
combined_batch[, family_name := aircraft(model_family)]

#Make one-time corrections for records with multiple model families
combined_batch[, max_fams := as.integer(length(unique(family_name))), by = c('serial_number')]
problem_serial_numbers <- unique(combined_batch[max_fams >1]$serial_number)
setorderv(combined_batch, cols = c('serial_number', 'report_date'))
unique(combined_batch[max_fams > 1, .(serial_number, family_name)])
combined_batch[serial_number == "0708038" & model == "HH-60L", model := 'CH-47F' ]
combined_batch[serial_number == "1520784" & model == "UH-72A", model := "UH-60M"]
combined_batch[serial_number == "9100259" & model == "C-26E", model := "CH-47D"]

#re-run the mapping to family name
for (i in 1:length(all_models)) {
  m <- all_models[i]
  while(substr(m,nchar(m),nchar(m)) %in% LETTERS){m <- substr(m,1,nchar(m)-1)}
  if(substr(m,nchar(m),nchar(m)) %in% sseq & substr(m,nchar(m)-1,nchar(m)-1) %in% LETTERS){
    m <- substr(m,1,nchar(m)-2)}
  combined_batch[model==all_models[i], model_family :=m]}
combined_batch[, family_name := aircraft(model_family)]
#combined_batch[family_name == 'Pave Hawk', family_name := 'Blackhawk']

'Data check for states'
#combined_batch[, state_original := state]
combined_batch[, state_id := ifelse(!is.na(facility_id), substr(facility_id,1,2) ,NA)]
duic <- fread('//equipment_readiness_path/Data/Original from sponsor/LOGSA/Second LOGSA visit/D_UIC.txt',na.strings = c(""))
duic <- unique(duic[,c('UIC', 'LOC_CD')])
for (i in 1:nrow(duic)) {combined_batch[uic == duic[i,1], state_duic:=duic[i,2]]}

'Data check for missing on-hand value'
print('Dates for which variable on_hand is missing')
sort(unique(combined_batch[ is.na(on_hand), report_date]))
combined_batch[, poss_hrs := as.double(`POSS HRS`)]


'Data check for empty fields that should instead be NA'
nnames <- names(combined_batch)[!grepl('report_date|REPORT DATE',names(combined_batch))]
for(name in nnames) {stopifnot(combined_batch[get(name)=='' |get(name)==' ',.N]==0)}

'Create new mission capable variables using provided values'
'See fxn documentation as needed'
 new_mc_variables <- c('nmcm', 'nmcs', 'pmc', 'pmcm', 'pmcs')
 for (v in new_mc_variables){
   V <- toupper(v)
   V_days_hrs <- paste0(V,' DAYS / HRS')
   combined_batch <- combine_fields(combined_batch, v, V, V_days_hrs)}

#create var nmc from the field "NMC", and fill in NA's with the sum of nmcs and nmcm
combined_batch[report_date == '2017-08-15', NMC := `POSS HRS` - MC] # 10/18/19 fix, see email
combined_batch[, nmc := as.double(NMC)]
combined_batch[ is.na(nmc), nmc := as.double(nmcs + nmcm)]

#create new nmc_perc field per the final model
combined_batch[, nmc_perc := round(nmc/poss_hrs, 4)]
combined_batch[ is.nan(nmc_perc), nmc_perc := NA]

#do something similar with fmc but impute with percent
combined_batch[, fmc := as.double(FMC)]
combined_batch[ is.na(fmc), fmc := as.double(`FMC %`*poss_hrs)] #impute here because it wasnt given for 07-10 FY's


'Check that sums equal'
d <- 1
'PMC = PMCS + PMCM'
stopifnot(combined_batch[!is.na(pmc) & !is.na(pmcs) & !is.na(pmcm), abs(pmc - (pmcs + pmcm)) < d])
'MC = FMC + PMC'
combined_batch[ !is.na(pmc) & !is.na(fmc) & !is.na(mc), mc_check := mc - (pmc + fmc)]
print(paste0('Number of elements that dont satisfy the condition: ', combined_batch[ mc_check > d, .N]))
print('Corresponding dates: ')
unique(combined_batch[ mc_check > d, report_date])
combined_batch[ mc_check > d, mc_check]
combined_batch[, mc_check := NULL]

'Data checks'
stopifnot(combined_batch[is.na(report_date), .N]==0) #missing report dates
stopifnot(combined_batch[nchar(serial_number) != 7, .N] == 0) #all serial numbers are 7 chars long
stopifnot(combined_batch[, sum(duplicated(serial_number)), by = report_date][V1 > 0, .N] == 0) #duplicate serial numbers in a month
stopifnot(combined_batch[!(day(report_date) ==15), .N] == 0) # whether the data/report was submitted on the 15th of the month

dataset_dates <- unique(combined_batch$report_date)
#stopifnot(seq.Date(min(dataset_dates),max(dataset_dates), by = "month") %in% dataset_dates)
seq.Date(min(dataset_dates),max(dataset_dates), by = "month")[!(seq.Date(min(dataset_dates),max(dataset_dates), by = "month") %in% dataset_dates)]

fname <- paste0('Processed_Aircraft_Readiness_FY2007_to_',format(max(combined_batch$report_date), "%Y%m"), '.csv')
fwrite(combined_batch, paste0(output_location, fname))
saveRDS(combined_batch,paste0(output_location,'Processed_Aircraft_Readiness_FY2007_to_',format(max(combined_batch$report_date), "%Y%m"),'.rds'))
fwrite(DataInventory(combined_batch),paste0(metadata_location,'Final DataInventory 2007 to ',format(max(combined_batch$report_date), "%Y%m"),'.csv'))
