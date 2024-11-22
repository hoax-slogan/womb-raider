import pandas as pd


def load_probe_ann():
    file = './jeg3_annotations/JEG3_ann_GPL96-57554.txt'
    df = pd.read_csv(file, delimiter="\t", quotechar='"', comment="#", engine="python")
    return df
