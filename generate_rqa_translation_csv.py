import argparse
import csv
import json

from core_data_modules.data_models import Message, CodeScheme
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger

Logger.set_project_name("WorldBank-PLR")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a CSV containing the relevant messages from a Coda "
                                                 "dataset, with empty columns where the translations can be "
                                                 "manually filled in")

    parser.add_argument("messages_file_path", metavar="messages-file-path",
                        help="Path to a labelled coda messages file for the dataset")
    parser.add_argument("code_scheme_file_path", metavar="code-scheme-file-path",
                        help="Path to the file for the code scheme to check for relevant messages under")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to write the translations CSV to")

    args = parser.parse_args()

    messages_file_path = args.messages_file_path
    code_scheme_file_path = args.code_scheme_file_path
    csv_output_file_path = args.csv_output_file_path

    log.info(f"Loading the code scheme from '{code_scheme_file_path}'...")
    with open(code_scheme_file_path) as f:
        code_scheme = CodeScheme.from_firebase_map(json.load(f))
    log.info(f"Loaded code scheme with scheme id '{code_scheme.scheme_id}'")

    log.info(f"Loading the messages from '{messages_file_path}'...")
    with open(messages_file_path) as f:
        messages = []
        for json_msg in json.load(f):
            messages.append(Message.from_firebase_map(json_msg))
    log.info(f"Loaded {len(messages)} messages")

    log.info(f"Filtering for relevant messages...")
    relevant_messages = []
    for msg in messages:
        # Get the latest label from each scheme
        latest_labels = dict()  # of scheme id -> label
        for label in msg.labels:
            if label.scheme_id not in latest_labels:
                latest_labels[label.scheme_id] = label
                
        # Determine if this message is relevant
        # A message is considered relevant if any of its latest, manually approved labels are normal codes.
        relevant = False
        for label in latest_labels.values():
            if label.code_id == "SPECIAL-MANUALLY_UNCODED":
                continue

            if not label.checked:
                continue
            
            if label.scheme_id.startswith(code_scheme.scheme_id):
                code = code_scheme.get_code_with_code_id(label.code_id)
                if code.code_type == CodeTypes.NORMAL:
                    relevant = True

        if relevant:
            relevant_messages.append(msg)
    log.info(f"{len(relevant_messages)} messages were relevant")

    log.info(f"Writing CSV to '{csv_output_file_path}'...")
    with open(csv_output_file_path, "w") as f:
        headers = ["MessageID", "Text", "English Translation", "Message Anonymised"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for msg in relevant_messages:
            writer.writerow({
                "MessageID": msg.message_id,
                "Text": msg.text,
                "English Translation": "",
                "Message Anonymised": ""
            })
    log.info("Done")
