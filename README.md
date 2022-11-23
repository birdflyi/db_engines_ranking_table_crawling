# db_engines_ranking_table_crawling
Crawling data from [DB-Engines](https://db-engines.com/en/ranking ), and auto update new changes into my manually labeled datasets as much as possible.

# 1. Crawling ranking table
Crawling the DBMS ranking data from [DB-Engines](https://db-engines.com/en/ranking ) with the beautifulsoup package.
save as [ranking_crawling_202211_raw.csv](./data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv)

# 2. reuse existing tagging info
Reuse existing tagging information manually labeled [DB_EngRank_tophalf_githubprj_summary.csv](./data/existing_tagging_info/DB_EngRank_tophalf_githubprj_summary.csv). 
Keep the manually labeled items of each record, update the new scores and new ranks, or insert new records.
Results will be saved as [ranking_crawling_202211_automerged.csv](./data/db_engines_ranking_table_full/ranking_crawling_202211_automerged.csv), 
and with a by-product [df_category_labels_updated.csv](./data/db_engines_ranking_table_full/df_category_labels_updated.csv) point out the 
mapping relations between the values of 'category_label' and 'Multi_model_info'.
