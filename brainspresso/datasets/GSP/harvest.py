
from brainspresso.sources.dataverse import Dataverse


KEYS = ("raw", "meta")

FILE_IDS = {
    "meta": {
        "GSP_list_140630.csv": "doi:10.7910/DVN/25833/1RIO8A",
        "GSP_retest_140630.csv": "doi:10.7910/DVN/25833/J3MVZX",
    },
    "raw": {
        "GSP_part1_140630.tar": "doi:10.7910/DVN/25833/D9DWAI",
        "GSP_part2_140630.tar": "doi:10.7910/DVN/25833/FLDSGW",
        "GSP_part3_140630.tar": "doi:10.7910/DVN/25833/MZ4PKQ",
        "GSP_part4_140630.tar": "doi:10.7910/DVN/25833/WYMY3W",
        "GSP_part5_140630.tar": "doi:10.7910/DVN/25833/QT8HGQ",
        "GSP_part6_140630.tar": "doi:10.7910/DVN/25833/HMLVBE",
        "GSP_part7_140630.tar": "doi:10.7910/DVN/25833/LV9K01",
        "GSP_part8_140630.tar": "doi:10.7910/DVN/25833/5RBER6",
        "GSP_part9_140630.tar": "doi:10.7910/DVN/25833/7AIUVE",
        "GSP_part10_140630.tar": "doi:10.7910/DVN/25833/RV7AJA",
        "GSP_retest_140630.tar": "doi:10.7910/DVN/25833/ZBV7LM",
    }
}


def sources() -> dict:
    """Sources of GSP: Harvard Dataverse (token required)"""
    return {"dataverse": Dataverse(FILE_IDS, "harvard")}


