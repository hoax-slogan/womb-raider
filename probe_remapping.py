import pandas as pd
import mygene


def merge_expr_data_with_cdf_descriptions():
    expr_data = pd.read_csv("normalized_expression_data.csv", index_col=0)
    ann = pd.read_csv("HGU133A_Hs_ENTREZG_desc.txt", sep="\t", index_col=0)
    merged_data = pd.merge(ann, expr_data, left_index=True, right_index=True)
    merged_data.to_csv("ann_norm_expr_data.csv")


def remap_entrez_id_to_gene_symbols():
    expr_data = pd.read_csv("ann_norm_expr_data.csv", index_col=0)
    mg = mygene.MyGeneInfo()
    gene_info = mg.querymany(expr_data.index, scopes='entrezgene', fields='symbol', species='human')
    gene_df = pd.DataFrame(gene_info)
    merged_data = expr_data.merge(gene_df[['query', 'symbol']], left_index=True, right_on='query', how='left')
    merged_data = merged_data.drop(columns=['query'])
    merged_data.index = merged_data.index.fillna('Unknown')
    merged_data.to_csv('jeg3_expression_data.csv', index=False)
