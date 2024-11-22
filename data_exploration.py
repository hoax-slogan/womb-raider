import pandas as pd

annotation_file = 'JEG3_ann_GPL96-57554.txt'

probe_annotations = pd.read_csv(annotation_file, sep='\t', comment='#')
print(probe_annotations.head())

xist_probes = probe_annotations[probe_annotations['Gene Symbol'].str.contains('XIST', case=False, na=False)]
sry_probes = probe_annotations[probe_annotations['Gene Symbol'].str.contains('SRY', case=False, na=False)]


print("XIST Probes:\n", xist_probes)
print("SRY Probes:\n",  sry_probes)
