known_columns = dict()
common_columns = ["description", "Material", "Organism", "project", "availability", "same as"]
known_columns['organism'] = [
    "Sex", "birth date", "breed", "health status", "birth location", "birth location longitude",
    "birth location latitude", "birth weight", "placental weight", "pregnancy length", "delivery timing",
    "delivery ease", "Child Of", "pedigree", "strain"
]
known_columns['specimen from organism'] = [
    "specimen collection date", "animal age at collection", "developmental stage", "health status at collection",
    "organism part", "specimen collection protocol", "fasted status", "number of pieces", "specimen volume",
    "specimen size", "specimen weight", "specimen picture url", "gestational age at sample collection"
]
known_columns['pool of specimens'] = [
    "pool creation date", "pool creation protocol", "specimen volume", "specimen size",
    "specimen weight", "specimen picture url"
]
known_columns['cell specimen'] = ["markers", "cell type", "purification protocol"]
known_columns['cell culture'] = [
    "culture type", "cell type", "cell culture protocol", "culture conditions", "number of passages"
]
known_columns['cell line'] = [
    "cell line", "biomaterial provider", "catalogue number", "number of passages", "date established", "publication",
    "cell type", "culture conditions", "culture protocol", "disease", "karyotype"
]