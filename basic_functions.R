
'author: christopher matthew sampah'
'date: 10-18-2019'
'purpose: 1) take several data.table functions and pair them down for simpler tasks; 2) create convenience
functions to perform tasks often performed in scrubbing and merging multiple datasets with differing
formats. Useful for basic to moderate data scrubbing that doesnt require verbose and numerous arguments'

require(data.table)
require(lubridate)

aircraft <- function(v, key = NA) {
  #get helicopter models in string format, return the corresponding family name
  if(is.na(key)) {
    key <- list('UH-60' ='Blackhawk',
                'CH-47' ='Chinook',
                'OH-58' ='Kiowa',
                'UH-72' ='Lakota',
                'AH-64' ='Apache',
                'C-12' ='Huron',
                'HH-60' ='Blackhawk',
                'UH-1' = 'Iroquois',
                'C-23' ='Sherpa',
                'UC-35' ='Citation V',
                'NA' = 'NA')}

  ##this function receives ONE element, like UC-35A, and returns Citation V using the key above;
  #it goes through the list names, matching on a name through regex, then obtains
  #the list element corresponding to that name
  temp.fxn <- function(model,this.key = key){
    match.val <- names(this.key)[unlist(sapply(names(this.key),function(x) {
      grepl(x,model)
    }))]

    if(length(match.val) == 0) {NA} #if there was no match
    else(this.key[[match.val]])}

  #apply the above fxn to an atomic type object
  unlist(lapply(v,function(x) {
    if(grepl(' ',x)) {
      x <- gsub(' ','',x)
      x <- paste0(substr(tester,1,2),'-',substr(tester,3, nchar(tester)))}
    if(is.na(x)) {NA}
    else if(nchar(x) < 6) {temp.fxn(x)}
    else(NA)}))}

clipboard <- function(){
  # copy something to the clipboard, run this function, and it'll return it in the console
  # particular to your regular windows filepath, it returns the filepath with the backslashes corrected
  gsub("\\\\","/",readClipboard()) }

create_nmc_table <- function(dataset, fixed_wing_aircraft) {
  # this function is specifically for the covariates script of the readiness data
  dt <- copy(dataset)
  fixed_wing <- fixed_wing_aircraft
  dt[, temp.col := '_'] #create this field to use in the aggregation table "t" below
  dt.temp <- dt[!is.na(nmc_perc) & !is.na(facility_id),]#for calcs in this section, filter missing nmc% and missing facility

  #obtain model-specific: aircraft count, nmc% mean & nmc% total per month per facility (not excluding any families)
  t1 <- setnames(dcast.data.table(dt.temp, model_family + facility_id + report_date ~ temp.col,
                                  value.var = c('nmc_perc', 'nmc_perc'), fun = list(length, sum)),
                 c('nmc_perc_length__','nmc_perc_sum__'),
                 c('N_model_family','sum_nmc_perc_model_family'))

  #obtain total aircraft count and total nmc% per month per facility (excluding fixed wing and lakota)
  t.total <- setnames(dcast.data.table(dt.temp[!(family_name %in% c('Lakota', fixed_wing)),], facility_id + report_date ~ temp.col,
                                       value.var = list('model_family', 'nmc_perc') , fun = list(length, sum)),
                      c('model_family_length__', 'nmc_perc_sum__'), c('N_all', 'sum_nmc_perc_all'))

  #merge the above two tables into one, then backshift report date 1-month, 2-months, 3-months
  #to merge onto readiness accordingly
  #we want to eventually calculate values for PRIOR month
  d <- merge(t1, t.total, by = c('facility_id', 'report_date'), all = TRUE)
  d[, report_date_next_month := report_date %m+% months(1)]
}

n.month.value <- function(data,feature,n, id = 'serial_number',
                          new.col.name = NA,
                          no.data = FALSE,
                          print.flag = TRUE) {
  #shift values forward or back in time by n_months
  dt <- as.data.table(copy(data))
  #disqualifying conditions
  if(n ==0) { print('n cannot be zero.')}
  if(new.col.name %in% names(dt)) {
    print(paste0('The name of the new column already corresponds to a feature in the dataset'))}
  stopifnot(n !=0)
  stopifnot(!(new.col.name %in% names(dt)))

  #moving on
  this.date <- 'report_date'
  dt[, eval(this.date) := as.Date(get(this.date))]
  #get the prior features
  prior.feature <- paste0('new_',feature)
  prior.date <- 'new_report_date'
  if(!is.na(new.col.name)) { prior.feature <- new.col.name}
  temp <- setnames(dt[, c(id, this.date, feature), with = FALSE], c(feature,this.date),
                   c(prior.feature,paste0(this.date,2)))
  temp[get(prior.feature) == 9999999.0, eval(prior.feature) := NA]
  temp <- unique(temp)
  n2 <- abs(n)
  if(n > 0) {dt[, new_report_date := report_date %m+% months(n2)]}
  if(n < 0) {dt[, new_report_date := report_date %m-% months(n2)]}
  dt <- merge(dt, temp, by.x = c(id, prior.date),
              by.y = c(id,paste0(this.date,2)), all.x = TRUE)
  if(print.flag) {
    print('Check the below values to ensure the output is desired:')
    print(head(unique(dt[,c(id, this.date, prior.date,feature,prior.feature), with = FALSE])))
  }

  dt[, eval(prior.date) := NULL]
  if(no.data) {print("")}
  else(unique(dt))}

three_month_average <- function(dataset,feature, id = 'serial_number',time.id = 'report_date',
                                delete_cols = F, new_cols_only = F, source_data = NA, new.name = NA) {
  this.id <- id
  f <- feature
  if(is.null(dim(source_data))) {
    source_data <- readRDS(paste0('//readiness_path/',
                                  'Data/Processed/Master aircraft readiness data from Devon Hall unduped/',
                                  'Processed_Aircraft_Readiness_FY2007_to_201909.rds'))}

  features <- paste0(c('last_month_', 'last_2_month_', 'last_3_month_', 'three_month_average_'),f)
  if(!is.na(new.name)) {features[4] <- new.name}

  dt.temp <- n.month.value(dataset,f,-1,id = this.id, print.flag = FALSE, new.col.name = features[1])
  dt.temp <- n.month.value(dt.temp,f,-2,id = this.id, print.flag = FALSE, new.col.name = features[2])
  dt.temp <- n.month.value(dt.temp,f,-3,id = this.id, print.flag = FALSE, new.col.name = features[3])
  dt.temp[, features[4] := rowMeans(dt.temp[,features[1:3], with = F],
                                    na.rm = TRUE)]
  dt.temp[, features[4] := round(get(features[4]),4)]
  if(delete_cols){dt.temp <- dt.temp[,c(this.id, time.id, f,features[4]), with = F]}
  else{dt.temp <- dt.temp[,c(this.id, time.id, f,features), with = F]}
  dt.temp <- unique(dt.temp)
  if(is.data.frame(dataset)){ final <- merge(dataset, dt.temp[, eval(f) := NULL], by = c(time.id, id), all.x = TRUE)}
  else{final <- dt.temp}
  if(new_cols_only) {final[,c(this.id, time.id, f,features), with = F]}
  else{final}

}

add.mapped.features <- function(data, map) {
  #take a dataframe and a list mapping new features to old features, then create those new features
  #from the old features they map from
  dt <- copy(data)
  new.cols <- names(map)

  for(new.col in new.cols) {
    current.col <- intersect(map[[new.col]], names(dt))
    if(length(current.col) >0) {dt[, eval(new.col) := get( current.col )]}
  }
  dt}

combine_fields <- function(dt,new_col, old_col_1, old_col_2, is.date=FALSE) {
  # a variable can have a different name in different file formats. Therefore, get
  # the 2 fields of that var (old_col_1 from file 1 and old_col_2
  # and combine them into one field (new_col). Convert to type 'date' if needed
  dt2 <- as.data.table(copy(dt))
  dt2[, eval(new_col) := get(old_col_1)]
  dt2[is.na(get(new_col)), eval(new_col) := get(old_col_2)]
  if (is.date){dt2[, eval(new_col) := as.Date(get(new_col),"%m/%d/%Y" )]}
  return(dt2)}

count.uniques <- function(data, cols=NA, return_unique = FALSE) {
  #count the number of unique records in a dataset, print dimensions before and after de-duplicating,
  #with the option to return the dataset with duplicates removed
  dataset <- as.data.table(data)
  if(all(is.na(cols))) {cols <- names(dataset)}
  print('Dimensions before removing duplicates:')
  print(dim(dataset[,cols, with = FALSE]))
  print('Dimensions after removing duplicates:')
  dt <- copy(unique(dataset))
  print(dim(dt[,cols, with = FALSE]))
  if(return_unique){dt}}

cwrite <- function(c.data, c.file, c.dir,rdata = FALSE) {
  # tailoring of fwrite for shorter, scripting
  if(substr(c.dir, nchar(c.dir), nchar(c.dir)) != '/') {c.dir <- paste0(c.dir,'/')}

  if(rdata) {saveRDS(object = c.data, file = paste0(c.dir,c.file,'.rds'))}
  else(fwrite(x = c.data, file = paste0(c.dir,c.file,'.csv')))}

left_merge <- function(datum1, datum2, merge.key) {
  if(length(merge.key) ==1) {merge(datum1,datum2,by.x = merge.key[[1]], by.y = merge.key[[1]], all.x = TRUE)}
  else(merge(datum1,datum2,by.x = merge.key[[1]], by.y = merge.key[[2]], all.x = TRUE))}

right_merge <- function(datum1, datum2, merge.key) {
  if(length(merge.key) ==1) {merge(datum1,datum2,by.x = merge.key[[1]], by.y = merge.key[[1]], all.y = TRUE)}
  else(merge(datum1,datum2,by.x = merge.key[[1]], by.y = merge.key[[2]], all.y = TRUE))}


print.tab.names <- function(list.of.files) {
  #if inputting a list of Excel filepaths, return the tab names of each file
  p <- list.of.files
  lapply(p, function(x){print(excel_sheets(x))})}

read.all.excel <- function(the_path, the.tab, combine = FALSE) {
  #go to a directory, read-in a dataset on an excel workbook tab, the tab determined by a keyword in its name,
  #then store these datasets in a list; if you want, you can just combine these datasets into one big dataset
  #after read-in
  print('The read-in order of files is (first to last):')
  file_names <- list.files(the_path)
  for(i in 1:length(file_names)) {print(file_names[i])}

  datasets <- lapply(list.files(the_path, full.names = TRUE),
                     function(s){
                       tab <- excel_sheets(s)
                       dt <- as.data.table(read_excel(s,sheet = tab[grepl(the.tab,tab)]))
                       print('The (row,column) dimensions of the data that was just read-in is')
                       t <- dim(dt)
                       print(paste0('(',t[1],',',t[2],')'))
                       nms <- names(dt)
                       if(grepl('\r\n',nms)) {
                         print('This file has the weird "\r\n" issue with the field names, for example:')
                         print(nms[grepl('\r\n',nms)])
                         dt <- setnames(dt, nms,gsub('\r\n','_', nms))
                       }


                       if(t[1] != dim(unique(dt))[1]) {print('This file has duplicate rows')}
                       fy_correct <- names(dt)[grepl('fy', names(dt))]
                       setnames(dt, fy_correct, toupper(fy_correct))
                       dt})

  dataset.names <- lapply(datasets, function(x){names(x)})
  if(length(unique(dataset.names)) > 1) {print('Not all files have the same columns')}
  if(combine) {
    t <- rbindlist(datasets, use.names = TRUE)
    if(dim(t)[1] != dim(unique(t))[1]) {
      print('Duplicate rows exist; they have been removed')
      unique(t)}
    else(t)
  }
  else(datasets)}

remove_sparse_columns <- function(data, percentage = NA) {
  #if a column of the data is missing a percentage of the time, remove it
  p <- 0
  if(is.na(percentage)) {p <- .95}
  else(p <- percentage)
  if(p > 1) { p <-1}
  print(paste('Sparsity is defined as at least',toString(p *100),'percent missing values in that column.'))
  dt <- as.data.frame(copy(data))
  print('Dimensions of data before removing sparse columns:')
  print(dim(dt))
  dt <- as.data.table(dt[,-which(colMeans(is.na(dt)) >= p)])
  if(dim(dt)[1] == 0) { dt <- as.data.frame(copy(data)) }
  print('Dimensions after:')
  print(dim(dt))
  dt}

summary_stats <- function (dataset, columns, condition=0, where) {
  'Again, I can make this more general'
  ## this function returns summary stats with the option of excluding NA or 0 values
  print('I need to do a check for numeric type columns and re-organize this fxn')
  for (col in columns) {
    if(!is.numeric(dt[,get(col)])) {
      stop('At least one of these columns is not of numeric type')}}

  checks <- columns
  which_check <- c('NA')
  N <- c('NA')
  rmses <- c('NA')
  qts <- c('5% quantile', '10% quantile', '90% quantile', '95% quantile')
  mean_col <- c('NA')
  median_col <- c('NA')
  min_col <- c('NA')
  max_col <- c('NA')
  desc <- ''
  dt <- data.table()
  for (col in checks) {
    if (condition ==1) {
      dt <- dataset[!is.na(get(col)) & abs(get(col)) > .Machine$double.eps] #data where difference !=0 and no NA
      desc <- '(exclude zero and missing (NA))'}

    else if(condition ==2) {
      dt <- dataset[!is.na(get(col)) & (abs(get(col)) > .Machine$double.eps | get(col)==0)] #data where difference != NA
      desc <- '(exclude missing (NA) only)'}

    else if(condition ==0) {dt <- dataset}

    which_check <- c(which_check, col) #to capture which check is having its summary stats calculated, serves as an extra check
    N <- c(N, nrow(dt))
    rmses <- c(rmses, sqrt(mean(dt[, get(col)^2]))  ) #the RMSE

    d <- dt[, get(col)]
    qts <- rbind(qts,quantile(d,c(0.05, 0.10,0.90, 0.95),type=1, na.rm=TRUE)  )
    mean_col <- c(mean_col,mean(d)  )
    median_col <- c(median_col, median(d)  )
    max_col <- c(max_col, max(d)  )
    min_col <- c(min_col, min(d)  )
  }

  s <- as.data.table(cbind(which_check,N,mean_col,median_col,max_col, min_col,rmses,qts))
  colnames(s) <- c('Check', 'N', 'Mean', 'Median','Maximum','Minimum', 'RMSE', ' 5%', '10%', '90%', '95%')
  fwrite(s[-1,], paste0(where, 'Summary Statistics ', desc, '  .csv'))}

