import CONFIG
import csv

# Returns a set of offices/units given a defined country
def map_country_to_units(country):
    target_units = set()
    with open(CONFIG.static_path_wrap("ug-region-country.csv")) as f:
        reader = csv.DictReader(f)
        # create a set of dicts from the csv file
        for row in reader:
            if row["Country"] == country:
                target_units.add(row["User group:"])

    return target_units
