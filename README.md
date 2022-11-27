# db_engines_ranking_table_crawling
Crawling data from [DB-Engines](https://db-engines.com/en/ranking ), and auto update new changes into my manually labeled datasets as much as possible.

# 1. Crawling ranking table
Crawling the DBMS ranking data from [DB-Engines](https://db-engines.com/en/ranking ) with the beautifulsoup package.
save as [ranking_crawling_202211_raw.csv](./data/db_engines_ranking_table_full/ranking_crawling_202211_raw.csv)

# 2. Crawling DBMS information
Crawling the DBMS information from the db-engines DBMS_insitelink, which has crawled by step "Crawling ranking table".

# 3. join ranking_table and dbms_info on 'DBMS'
Join ranking_table and dbms_info on 'DBMS' of ranking_table and 'Name' of dbms_info. Set the key name alias to 'DBMS' after joined.
Default set use_cols_ranking_table = None to use all fields of ranking_table, and set
use_cols_dbms_infos = ["Developer", "Name", "Description", "Initial release", "Current release", "License",
                               "Cloud-based only"] to use part of dbms_info.

# 4. recalc ranking_table_dbms_info
The table joined by ranking_table and dbms_info is marked as ranking_table_dbms_info. Some fields should be re-calculated as other data formats.
Default set recalc_cols = ["Initial release", "Current release", "License", "Cloud-based only"] and a correspond function must be implemented in class RecalcFuncPool() for each re-calculate filed.

# 5. reuse existing tagging info
Reuse existing tagging information manually labeled [DB_EngRank_tophalf_githubprj_summary.csv](./data/existing_tagging_info/DB_EngRank_tophalf_githubprj_summary.csv). 
Keep the manually labeled items of each record, update the new scores and new ranks, or insert new records.
Results will be saved as [ranking_crawling_202211_automerged.csv](./data/db_engines_ranking_table_full/ranking_crawling_202211_automerged.csv), 
and with a by-product [df_category_labels_updated.csv](./data/db_engines_ranking_table_full/category_labels_updated.csv) point out the 
mapping relations between the values of 'category_label' and 'Multi_model_info'.


---
# Introduce to the crawling task
Target: 基于github_log的数据库开源软件生态调查
- [DB-Engines Ranking中的DBMS分类和采样](https://www.yuque.com/g/zhlou/ln7m80/uqvtd2/collaborator/join?token=AB0FFMTlCGjjED8q)
- [The calculation method of DB-Engines Ranking scores](https://www.yuque.com/zhlou/ln7m80/pfrgfv)

