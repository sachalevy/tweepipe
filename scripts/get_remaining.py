import json
import os
import argparse


def main(output_folder: str, source: str, output_file: str):
    """
    Retrieve missing twitter handles.

    Args:
        output_folder (str): Path to folder with files named after the retrieved users.
        source (str): Path to file containing list of users to be retrieved.
        output_file (str): Path to output file where remaining users should be stored.
    """
    get_twitter_handle = lambda x: x.replace(".json", "")
    scraped_twitter_handles = list(map(get_twitter_handle, os.listdir(output_folder)))

    with open(source, "r") as f:
        source_twitter_handles = json.load(f)

    remaining_twitter_handles = set(source_twitter_handles) - set(
        scraped_twitter_handles
    )
    with open(output_file, "w") as f:
        json.dump(list(remaining_twitter_handles), f)

    print(
        f"Saved {len(remaining_twitter_handles)} remaining twitter handles to {output_file}."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load user(s) followers.")
    parser.add_argument(
        "--output-folder",
        type=str,
        help="Path to folder containing files with naming like TWITTER_HANDLE.json.",
        required=True,
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Path to file containing all original twitter handles to scrape for.",
        required=True,
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path to file where remaining twitter handles should be stored.",
        required=True,
    )

    args = parser.parse_args()
    output_folder = args.output_folder
    source = args.source
    output_file = args.output_file

    main(output_folder, source, output_file)
