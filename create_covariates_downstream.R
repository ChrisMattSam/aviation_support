'christopher sampah'

source(paste0('//readiness_path/',
              'Analysis/Functions/csampah.ida/R/basic_functions.R'))
library(data.table)
library(plyr)

create_deployment_affected_covariates <- function(data){
  original_cols <- c("facility_id","serial_number","report_date","TASMG","uic", "model","hours_flown",
                     "facility_uic","facility_sq_feet","facility_quality","facility_age","family_name",
                     "nmc_perc", "reset_pct","reset", "depot_weight", "deployed_helicopter", 
                     "aircraft_dep_in_1_month_or_less","aircraft_dep_in_3_months_or_less", 
                     "aircraft_dep_in_6_months_or_less","aircraft_dep_in_12_months_or_less","prev_month_hours_flown",
                     "prev_month_total_airframe_hours","hours_flown_3_month_mean")
  dt <- as.data.table(data)
  
  ic <- c() #vector to keep track of covariates we want in the final output
  fixed_wing <- c('Huron','Citation V', 'Sherpa') # these need to be excluded from the creation of the covariates
  if( !('deployed_helicopter' %in% names(dt))) {
    dt[, deployed_helicopter := F]
    dt[(STATUS %in% c('mobilized', 'mob','deployed')) | (assign_func_code == 'DEP'), deployed_helicopter := T]
    
    ic <- c(ic,'deployed_helicopter')
  }
  if( !('total_airframe_hours' %in% names(dt)) & ('prev_month_total_airframe_hours' %in% names(dt))) {
    dt <- n.month.value(dt, 'prev_month_total_airframe_hours',1, new.col.name = 'total_airframe_hours',
                        print.flag = F)
  }
  if( !('model_family' %in% names(dt))) { dt[, model_family := substr(model,1, nchar(model)-1)]}
  
  # mean hours flown and mean airframe hours of all aircraft at a facility one month prior ####
  # create the mean values for this facility month, then shift back by 1 month
  
  dt[,mean_af_hours := .SD[!(family_name %in% fixed_wing) & deployed_helicopter == F,
                          mean(total_airframe_hours, na.rm = T)], by = .(report_date, facility_id)]
  
  dt[, sum_hf := .SD[!(family_name %in% fixed_wing) & deployed_helicopter == F, sum(hours_flown, na.rm = T)],
     by = .(report_date, facility_id)]
  
  dt[, mean_hf := .SD[!(family_name %in% fixed_wing) & deployed_helicopter == F, mean(hours_flown, na.rm = T)],
     by = .(report_date, facility_id)]
  
  new.name <- c('facility_mean_airframe_hours_prev_month','facility_mean_hours_flown_prev_month','facility_3_month_mean_hours_flown')
  
  dt <- n.month.value(dt, n = -1, id = 'facility_id', feature = 'mean_af_hours',
                      new.col.name = new.name[1], print.flag = F)
  dt <- n.month.value(dt, n = -1, id = 'facility_id',
                      feature = 'mean_hf', new.col.name = new.name[2], print.flag = F)
  dt <- three_month_average(dt,'sum_hf', id = 'facility_id',time.id = 'report_date', source_data = dt,
                            new.name = new.name[3])
  ic <- c(ic, new.name)
  
  # for each aircraft model_family and facility month, create mean nmc % for previous month####
  #then calculate mean nmc% excluding that model family and fixed wing
  #Case 1.1: aircraft is at the same facility next month and keeps the same family
  #Case 1.2: aircraft is at the same facility next month and switches family
  #Case 2: aircraft is at a different facility next month (base case)
  dt[nmc_perc > 1, nmc_perc :=1] #correct nmc values greater than
  
  t <-  setnames(create_nmc_table(dt, fixed_wing_aircraft = fixed_wing), 'report_date', 'report_date2')
  dt <- merge(dt,t, by.x = c('facility_id', 'report_date','model_family'),
              by.y = c('facility_id', 'report_date_next_month','model_family'), all.x = TRUE)
  for( f in c('facility_id', 'model_family', 'nmc_perc')) {
    dt <- n.month.value(dt, f,-1, new.col.name = paste0(f,'_last_month'), print.flag = F)}
  
  dt[, mean_fam_nmc_perc_prior_facility_month := sum_nmc_perc_model_family/(N_model_family)]
  dt[(facility_id == facility_id_last_month) & (model_family == model_family_last_month),
     mean_fam_nmc_perc_prior_facility_month := (sum_nmc_perc_model_family-nmc_perc_last_month)/(N_model_family-1)]
  
  #calculate mean nmc % excluding the model family of interest, and excluding fixed wing
  dt[N_model_family == N_all, mean_non_fam_nmc_perc_prior_facility_month:= 0]
  dt[N_model_family != N_all,
     mean_non_fam_nmc_perc_prior_facility_month:= (sum_nmc_perc_all - sum_nmc_perc_model_family)/(N_all - N_model_family)]
  dt[, sum_nmc_perc_non_model_family := sum_nmc_perc_all - sum_nmc_perc_model_family]
  
  #correct NA values
  dt[is.na(mean_fam_nmc_perc_prior_facility_month), mean_fam_nmc_perc_prior_facility_month := 0]
  dt[is.na(mean_non_fam_nmc_perc_prior_facility_month), mean_non_fam_nmc_perc_prior_facility_month := 0]
  
  ic <- c(ic,'mean_fam_nmc_perc_prior_facility_month','mean_non_fam_nmc_perc_prior_facility_month')
  
  # create the three month means for fam & non-fam nmc perc ####
  dt[,sum_nmc_perc := .SD[deployed_helicopter == F, sum(nmc_perc, na.rm = T)], by = .(facility_id, report_date)]
  dt[,sum_nmc_perc_model_family := .SD[deployed_helicopter == F , sum(nmc_perc, na.rm = T)], 
     by = .(facility_id, report_date,model_family)]
  dt[, sum_nmc_perc_non_model_family := sum_nmc_perc - sum_nmc_perc_model_family]
  dt[, n_all := .SD[deployed_helicopter == F, .N], by = .(facility_id, report_date)]
  
  dt[, n_model_family := .SD[deployed_helicopter == F & model_family == model_family, .N], by = .(facility_id, report_date, model_family)]
  dt[, n_non_model_family := n_all - n_model_family]
  
  #omit that specific serial number's values, if any
  dt[!is.na(nmc_perc), sum_nmc_perc := sum_nmc_perc - nmc_perc]
  dt[!is.na(nmc_perc), sum_nmc_perc_model_family := sum_nmc_perc_model_family - nmc_perc]
  dt[sum_nmc_perc_model_family < 0 , sum_nmc_perc_model_family := 0]
  dt[n_all ==1, sum_nmc_perc := 0]
  dt[n_model_family == 1, sum_nmc_perc_model_family := 0]
  dt[n_all > 1, n_all := as.integer(n_all - 1)]
  dt[n_model_family > 1, n_model_family := as.integer(n_model_family - 1)]
  
  #adjust nmc percent, counts for fixed wing
  dt[, V1 := .SD[deployed_helicopter == F & family_name %in% fixed_wing, sum(nmc_perc, na.rm = T)], 
     by = .(facility_id, report_date)]
  
  dt[, sum_nmc_perc := sum_nmc_perc - V1]
  dt[!(family_name %in% fixed_wing),
     sum_nmc_perc_non_model_family := sum_nmc_perc_non_model_family - V1]
  dt[ sum_nmc_perc_non_model_family < 0 , sum_nmc_perc_non_model_family := 0]
  dt[ sum_nmc_perc < 0, sum_nmc_perc :=0]
  dt$V1 <- NULL
  
  #countS
  dt[, fixed_wing_count := .SD[(deployed_helicopter == F) & (family_name %in% fixed_wing), .N], by = .(facility_id, report_date)]
  dt[, fixed_wing_count := fixed_wing_count - 1]
  dt[!(family_name %in% fixed_wing) , n_non_model_family := as.integer(n_non_model_family - fixed_wing_count)]
  dt$fixed_wing_count <- NULL
  
  #calculate prev 1 month, 2 month, and 3 month vals
  for ( i in 1:3) {
    n <- -i
    ending <- paste0('_',toString(i),'_months_ago')
    if( i ==1) {ending <- paste0('_',toString(i),'_month_ago')}
    
    for(f in c('nmc_perc','facility_id', 'model_family','sum_nmc_perc_model_family','n_model_family',
               'sum_nmc_perc', 'sum_nmc_perc_non_model_family', 'n_non_model_family')) {
      dt <- n.month.value(dt,f,n, new.col.name = paste0(f,ending), print.flag = F)}
    
  }
  
  #adjust for aircraft switching model family
  for( feature in sort(names(dt)[grepl('sum_nmc_perc_model_family|n_all_|n_model_family_|nmc_perc_', names(dt))])) {
    dt[is.na(get(feature)),eval(feature) := 0]}
  
  dt[, numerator := sum_nmc_perc_model_family_1_month_ago + sum_nmc_perc_model_family_2_months_ago + 
       sum_nmc_perc_model_family_3_months_ago ]
  
  dt[, denominator := n_model_family_1_month_ago + n_model_family_2_months_ago + 
       n_model_family_3_months_ago]
  
  dt[, numerator_2 := sum_nmc_perc_non_model_family_1_month_ago + sum_nmc_perc_non_model_family_2_months_ago +
       sum_nmc_perc_non_model_family_3_months_ago]
  
  dt[, denominator_2 := n_non_model_family_1_month_ago + n_non_model_family_2_months_ago + 
       n_non_model_family_3_months_ago]
  
  dt[,three_month_mean_fam_nmc_perc := numerator/denominator]
  dt[,three_month_mean_non_fam_nmc_perc := numerator_2/denominator_2]
  
  dt[denominator == 0, three_month_mean_fam_nmc_perc := 0]
  dt[denominator_2== 0, three_month_mean_non_fam_nmc_perc := 0]
  
  dt[three_month_mean_fam_nmc_perc > 1, three_month_mean_fam_nmc_perc := 1]
  ic <- c(ic,c('three_month_mean_non_fam_nmc_perc','three_month_mean_fam_nmc_perc'))
  
  # count of same model's helicopters, and different model's helicopters, at that facility for that month ####
  #count of same model's helicopters
  dt[, same_fam := .SD[deployed_helicopter == F,.N], by = .(facility_id, model_family,report_date)]
  dt[, total := .SD[deployed_helicopter == F & !(family_name %in% fixed_wing) ,.N],
     by = .(facility_id, report_date)]
  dt[, diff_fam := total-same_fam]
  nms <- c('same_fam_at_same_facility_count', 'diff_fam_at_same_facility_count')
  setnames(dt, c('same_fam', 'diff_fam'), nms)
  
  ic <- c(ic,nms)

  dt[, c(names(data), ic), with = F]
}
