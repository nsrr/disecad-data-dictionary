ver="0.1.0"

library(readr)
library(dplyr)

data_path <- "/Volumes/BWH-SLEEPEPI-NSRR-STAGING/20250325-dise/original-data//PennSleepSurgeryData-DCIExport_DATA_2025-04-17_1622.csv"
df <- read.csv(data_path, stringsAsFactors = FALSE)

# keep only the first instance
filtered_df <- df[df$redcap_repeat_instance == 1 | df$redcap_repeat_instrument == "", ]

# rename index nsrrid
filtered_df$nsrrid <- paste0("DC-1", sprintf("%02d", filtered_df$index))
filtered_df$index <- NULL

# data might in different redcap insturment. merge all into one row
merged_list <- list()
unique_nsrrids <- unique(filtered_df$nsrrid)

for (id in unique_nsrrids) {
  rows <- filtered_df[filtered_df$nsrrid == id, ]
  
  if (nrow(rows) == 1) {
    merged_list[[id]] <- rows
  } else {
    merged_row <- rows[1, ]
    if (nrow(rows) > 1) {
      for (i in 2:nrow(rows)) {
        for (col in names(rows)) {
          if (col != "nsrrid" && col != "redcap_repeat_instrument") {
            if ((is.na(merged_row[[col]]) || merged_row[[col]] == "") && 
                (!is.na(rows[i, col]) && rows[i, col] != "")) {
              merged_row[[col]] <- rows[i, col]
            }
          }
        }
      }
    }
    merged_list[[id]] <- merged_row
  }
}

merged_df <- do.call(rbind, merged_list)
merged_df$redcap_repeat_instrument <- NULL

# read data from race___0 to race___7 to a single variable, race
merged_df$race <- NA 

determine_race <- function(row) {
  race0_idx <- which(names(merged_df) == "race___0")
  race1_idx <- which(names(merged_df) == "race___1")
  race2_idx <- which(names(merged_df) == "race___2")
  race3_idx <- which(names(merged_df) == "race___3")
  race4_idx <- which(names(merged_df) == "race___4")
  race5_idx <- which(names(merged_df) == "race___5")
  race6_idx <- which(names(merged_df) == "race___6")
  race7_idx <- which(names(merged_df) == "race___7")
  
  # American Indian/Alaska Native (race___1 or race___2)
  if ((!is.na(row[race2_idx]) && row[race2_idx] == 1) || 
      (!is.na(row[race1_idx]) && row[race1_idx] == 1)) {
    return(1)
  }
  # Asian (race___3)
  else if (!is.na(row[race3_idx]) && row[race3_idx] == 1) {
    return(2)
  }
  # Native Hawaiian or Other Pacific Islander (race___6)
  else if (!is.na(row[race6_idx]) && row[race6_idx] == 1) {
    return(3)
  }
  # Black or African American (race___0)
  else if (!is.na(row[race0_idx]) && row[race0_idx] == 1) {
    return(4)
  }
  # White (race___4)
  else if (!is.na(row[race4_idx]) && row[race4_idx] == 1) {
    return(5)
  }
  # More than one race (race___5)
  else if (!is.na(row[race5_idx]) && row[race5_idx] == 1) {
    return(6)
  }
  # Unknown (race___7)
  else if (!is.na(row[race7_idx]) && row[race7_idx] == 1) {
    return(7)
  }
  # If multiple race boxes are checked
  else {
    race_idxs <- c(race0_idx, race1_idx, race2_idx, race3_idx, race4_idx, race6_idx, race7_idx)
    checked_races <- sum(row[race_idxs] == 1, na.rm = TRUE)
    if (checked_races > 1) {
      return(6)  # More than one race
    } else {
      return(7)  # Unknown or not reported
    }
  }
}

for (i in 1:nrow(merged_df)) {
  merged_df$race[i] <- determine_race(merged_df[i, ])
}


# drop original race___X variables
race_vars <- grep("^race___", names(merged_df), value = TRUE)
merged_df <- merged_df[, !(names(merged_df) %in% race_vars)]

# reorder columns
all_cols <- names(merged_df)
first_cols <- c("nsrrid", "sex", "ethnicity", "race")
remaining_cols <- setdiff(all_cols, first_cols)
new_col_order <- c(first_cols, remaining_cols)
merged_df <- merged_df[, new_col_order]





# remove variables that are completely empty 
merged_df <- merged_df[, sapply(merged_df, function(x) {
  if(is.character(x)) {
    sum(!is.na(x) & x != "") > 0
  } else {
    sum(!is.na(x)) > 0
  }
})]




# find non-numeric variables
identify_non_numeric_vars <- function(data) {
  non_numeric_vars <- c()
  var_types <- c()
  
  for (col_name in names(data)) {
    if (!is.numeric(data[[col_name]]) && !is.integer(data[[col_name]])) {
      non_numeric_vars <- c(non_numeric_vars, col_name)
      var_types <- c(var_types, class(data[[col_name]])[1])
    }
  }
  
  if (length(non_numeric_vars) > 0) {
    result <- data.frame(
      variable = non_numeric_vars,
      type = var_types,
      stringsAsFactors = FALSE
    )
    return(result)
  }
}

non_numeric_variables <- identify_non_numeric_vars(merged_df)

vars_to_keep <- c("nsrrid", "iq_sleep_bedtime", "iq_sleep_waketime", 
                  "iq_lt_comments", "iq_scope_media", "ss_surg_yes", 
                  "ss_hgn", "svdise_surg_yes", "dise_pcrit", "dise_phopa", 
                  "dise_pcrit_other", "dise_phopa_other", "dise_bl2phop", 
                  "dise_dci_ntp_bl1", "dise_dci_ntp_stim1", "dise_dci_vphop_stim1", 
                  "dise_dci_timing_stim1", "dise_dci_ntp_bl2", "surgery_procedurename", 
                  "surgery_cpt_other", "r_scope_media", "ad_percdiff_e", 
                  "ad_percdiffp_e", "ad_pybot", "ad_lmbot", "ad_vtbot", "ad_avgbot")

numeric_cols <- names(merged_df)[sapply(merged_df, function(x) is.numeric(x) | is.integer(x))]

cols_to_keep <- unique(c(numeric_cols, vars_to_keep))

missing_vars <- vars_to_keep[!vars_to_keep %in% names(merged_df)]

existing_vars_to_keep <- vars_to_keep[vars_to_keep %in% names(merged_df)]
cols_to_keep <- unique(c(numeric_cols, existing_vars_to_keep))

merged_df <- merged_df[, cols_to_keep, drop = FALSE]




# add dise_edf_filename column 
edf_dir <- "/Volumes/BWH-SLEEPEPI-NSRR-STAGING/20250325-dise/original-data/EDFs"
edf_files <- list.files(edf_dir, pattern = "\\.(edf|txt)$", ignore.case = TRUE, full.names = FALSE)

edf_mapping <- data.frame(
  filename = edf_files,
  stringsAsFactors = FALSE
)

edf_mapping$extracted_id <- gsub("(^.*?)(-confirmed-missing\\.txt|\\.edf|\\.txt)$", "\\1", edf_mapping$filename, ignore.case = TRUE)

merged_df$dise_edf_filename <- NA_character_

for (i in 1:nrow(merged_df)) {
  current_nsrrid <- merged_df$nsrrid[i]
  
  matching_files <- edf_mapping$filename[edf_mapping$extracted_id == current_nsrrid]
  
  if (length(matching_files) > 0) {
    merged_df$dise_edf_filename[i] <- matching_files[1]
  }
}




merged_df$visit <- 1

if (all(c("nsrrid", "visit") %in% names(merged_df))) {
  all_cols <- names(merged_df)
  other_cols <- all_cols[!all_cols %in% c("nsrrid", "visit")]
  new_col_order <- c("nsrrid", "visit", other_cols)
  merged_df <- merged_df[, new_col_order]}

write.csv(merged_df, "/Volumes/BWH-SLEEPEPI-NSRR-STAGING/20250325-dise/original-data/PennSleepSurgeryData-DCIExport_DATA_2025-04-17_1622_deidentified.csv", row.names = F, na='')




#harmonized dataset
harmonized_data<-merged_df[,c("nsrrid", "visit","iq_age","race","sex","ethnicity","iq_bmi","iq_systolicbp","iq_diastolicbp")]%>%
  dplyr::mutate(nsrrid=nsrrid,
                nsrr_age=iq_age,
                nsrr_race=dplyr::case_when(
                  race==1 ~ "american indian or alaska native",
                  race==2 ~ "asian",
                  race==3 ~ "native hawaiian or other pacific islander",
                  race==4 ~ "black or african american",
                  race==5 ~ "white",
                  race==6 ~ "multiple",
                  TRUE ~ "not reported"
                ),
                nsrr_sex=dplyr::case_when(
                  sex==0 ~ "male",
                  sex==1 ~ "female",
                  TRUE ~ "not reported"
                ),
                nsrr_ethnicity=dplyr::case_when(
                  ethnicity==0 ~ "hispanic or latino",
                  ethnicity==1 ~ "not hispanic or latino",
                  TRUE ~ "not reported"
                ),
                nsrr_bmi = iq_bmi,
                nsrr_bp_diastolic=iq_diastolicbp,
                nsrr_bp_systolic=iq_systolicbp) %>% select(nsrrid, visit, nsrr_age, nsrr_race, nsrr_sex, nsrr_ethnicity, nsrr_bmi,nsrr_bp_diastolic,nsrr_bp_systolic)

write.csv(harmonized_data,file = "/Volumes/BWH-SLEEPEPI-NSRR-STAGING/20250325-dise/nsrr-prep/0.1.0.pre/disecad-harmonized-dataset-0.1.0.csv", row.names = FALSE, na='')


