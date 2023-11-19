#!/usr/bin/env python

import pandas as pd

OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/"

# Reading the RW MAG merged file from openAlex
df_oa_rw = pd.read_csv(OUTDIR+"works_ids_RW_MAG_OA_merged_sample_BasedOndoi_ORpmid_ORmag.csv")

# Reading open alex works authorships file
df_oa_authors = pd.read_csv("/scratch/sm9654/openalex_snapshot_oct2023/csv-files/works_authorships.csv.gz")

# extracting relevant work ids
oa_work_ids = df_oa_rw['work_id'].unique()

# Only extracting authors from relevant rw works
df_oa_rw_authors = df_oa_authors[df_oa_authors['work_id'].isin(oa_work_ids)]

df_oa_rw_authors.to_csv(OUTDIR+"authors_ids_RW_MAG_OA.csv", index=False)

print("Number of RW-MAG-OA authors:", df_oa_rw_authors['author_id'].nunique())