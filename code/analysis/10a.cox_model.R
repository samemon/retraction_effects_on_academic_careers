library("survival")
library("survminer")

# Reading in data with multiple retractions
data <- read.csv("Cox_Model_Data_w_mult_retractions.csv")

# For the main models, we only consider single retractions
data_single <- data[which(data$nRetracted == 1),]

# Clustering on ID, experience field: log_cumCitations
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_single)
summary(res.cox)

# Clustering on ID, experience field: log_cumCollaborators
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCollaborators + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_single)
summary(res.cox)

# Clustering on ID, experience field: cumPapers
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_single)
summary(res.cox)

# Clustering on ID, experience field: cumPapers + log_cumCollaborators + log_cumCitations
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + 
                   log_cumCollaborators + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_single)
summary(res.cox)

# Now, running the same set of models, but including multiple retractions

# Clustering on ID, experience field: log_cumCitations
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data)
summary(res.cox)

# Clustering on ID, experience field: log_cumCollaborators

res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCollaborators + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data)
summary(res.cox)

# Clustering on ID, experience field: cumPapers

res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data)
summary(res.cox)

# Clustering on ID, experience field: cumPapers + log_cumCollaborators + log_cumCitations

res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + 
                   log_cumCollaborators + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data)
summary(res.cox)

# Now, running the same set of models (on single retractions), but only including first and last authors
data_first_last <- data_single[which(data_single$AuthorOrder_Medium == 0),]

# Clustering on ID, experience field: log_cumCitations
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_first_last)
summary(res.cox)

# Clustering on ID, experience field: log_cumCollaborators
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + log_cumCollaborators + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_first_last)
summary(res.cox)

# Clustering on ID, experience field: cumPapers
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_first_last)
summary(res.cox)

# Clustering on ID, experience field: cumPapers + log_cumCollaborators + log_cumCitations
res.cox <- coxph(Surv(start, stop, AttritionClass) ~ Retracted + Gender + AffRank + cumPapers + 
                   log_cumCollaborators + log_cumCitations + cohort_yr +
                   art + biology + business + chemistry + computer.science + economics + engineering + 
                   environmental.science + geography + geology + 
                   history + materials.science + mathematics + medicine + 
                   philosophy + physics + political.science + psychology + sociology + cluster(AID), 
                 id = AID,
                 data =  data_first_last)
summary(res.cox)

