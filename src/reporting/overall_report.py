import json
from src.utils.json_utils import make_json_serializable

def save_overall_report(report):
    report = make_json_serializable(report)

    with open("outputs/summaries/overall_summary.json", "w") as f:
        json.dump(report, f, indent=2)
