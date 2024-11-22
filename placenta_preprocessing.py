import re
import pandas as pd
import numpy as np
from io import StringIO
import anndata as ad


def load_metadata():
    metadata_dict = {}
    data_lines = []
    table_data_started = False
    metadata_path = r"C:\Users\chaos\OneDrive\Desktop\Projects\jeg3 experiment\primary_placental_data\GSE89497_series_matrix.txt"

    with open(metadata_path, 'r') as file:
        for line in file:

            line = re.sub(r',,', ',', line).strip()

            # Process metadata lines to form columns (ignoring redundant fields)
            if line.startswith('!Sample_') and "geo_accession" not in line:
                parts = line.split("\t")
                key = parts[0][1:]  # Remove '!' from the key
                values = parts[1:]
                metadata_dict[key] = values

            # Check if table data begins
            if line == "!series_matrix_table_begin":
                table_data_started = True
                continue
            elif line == "!series_matrix_table_end":
                break

            if table_data_started:
                data_lines.append(line)

    metadata_df = pd.DataFrame(metadata_dict)

    if data_lines:
        table_data_str = "\n".join(data_lines)
        df = pd.read_csv(StringIO(table_data_str), delimiter="\t", quotechar='"', engine='python')

        # Transpose to align ID REFs with metadata rows
        df = df.set_index("ID_REF").transpose().reset_index()
        df.columns.name = None  # Clear the index name for clarity

        # Concatenate metadata columns with data table
        combined_df = pd.concat([df, metadata_df], axis=1)
        combined_df.rename(columns={'index': 'ID_REF'}, inplace=True)
        # Removes extraneous quotes from cells of df
        combined_df = combined_df.map(lambda x: x.strip('"') if isinstance(x, str) else x)
    else:
        print("No sample data found")
    return combined_df


def convert_mdf_to_adata(combined_df):
    combined_df.set_index('Sample_title', inplace=True)
    m_adata = ad.AnnData(obs=combined_df)
    m_adata.X = np.zeros((m_adata.n_obs, 0))
    m_adata.obs_names_make_unique()
    return m_adata


def load_primary_data():
    data_path = r"C:\Users\chaos\OneDrive\Desktop\Projects\jeg3 experiment\primary_placental_data\GSE89497_Human_Placenta_TMP_V2.txt"
    gene_data = pd.read_csv(data_path, sep="\t", index_col=0)
    adata = ad.AnnData(X=gene_data.T)
    adata.var_names = gene_data.index     # Gene symbols
    adata.obs_names = gene_data.columns   # Sample IDs
    adata.obs_names_make_unique()
    return adata


def sort_rename_EVT_samples(adata):
    """
    Renames EVT samples to include subtype suffixes

    Parameters:
        p_adata: AnnData object with obs_names containing EVT samples.

    Returns:
        The same AnnData object with renamed EVT samples.
    """
    EVT_ranges = {
        "HE24W_EVT1": 40,
        "HE24W_EVT2": 40,
        "HE24W_EVT3": 40,
        "HE24W_EVT4": 40,
        "HE24W_EVT5": 40
    }

    # Initialize the counter for each subtype
    evt_counters = {subtype: 1 for subtype in EVT_ranges}

    # Iterate over each sample name in the adata object
    new_obs_names = []

    for obs_name in adata.obs_names:
        # Check if it's an EVT sample without a specific subtype label
        if obs_name.startswith("HE24W_EVT_sc"):
            # Determine the subtype based on the current counter ranges
            for subtype, limit in EVT_ranges.items():
                if evt_counters[subtype] <= limit:
                    new_name = f"{subtype}_sc{evt_counters[subtype]}"
                    evt_counters[subtype] += 1
                    break
            new_obs_names.append(new_name)
        else:
            # Append non-EVT names as-is
            new_obs_names.append(obs_name)

    # Update obs_names with the new names
    p_adata = adata
    p_adata.obs_names = new_obs_names
    p_adata.obs_names_make_unique()

    return p_adata


def combine_p_and_m_adata(m_adata, p_adata):
    scrna_adata = ad.concat([p_adata, m_adata], join='outer')
    print(scrna_adata)
    print(scrna_adata.obs.head())
    print(scrna_adata.var.head())
    return scrna_adata
