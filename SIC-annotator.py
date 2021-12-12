from bs4 import BeautifulSoup
import json
import os
import requests
from tqdm import tqdm

path = "./"

corpus = open(f"{path}/sec_corpus_2016-2019.jsonl").read()
corpus_raw = [json.loads(line) for line in tqdm(corpus.splitlines())]

replace_dict = {}

with open(f"{path}/SIC-annotated.jsonl", "w") as SIC:
    for line in tqdm(corpus_raw):

        source_raw = line["source"]
        source = source_raw.split("/")

        for file in os.listdir(f"{path}/data/{source[0]}/{source[1]}/{source[2]}"):
            if file.endswith(".html"):

                parse_file = open(f"{path}/data/{source[0]}/{source[1]}/{source[2]}/{file}", "r")
                index = parse_file.read()
                parse = BeautifulSoup(index, "html.parser")

                result = parse.select("a[href*=SIC]")

                try:
                    line["source"] = result[0].text

                except IndexError:

                    if line["source"] in replace_dict.keys():
                        line["source"] = replace_dict[line["source"]]

                    else:

                        company_name = parse.find("span", {"class": "companyName"})
                        name = company_name.text.split("\n")
                        final_name = name[0].replace("(Filer)", "").replace(",", "").strip()

                        if final_name[-1] == ".":
                            final_name = final_name[:-1]

                        url = final_name.replace(" ", "-")

                        lookup = requests.get(f"https://siccode.com/business/{url}")
                        lookup_soup = BeautifulSoup(lookup.content, "html.parser")

                        try:
                            lookup_tags = lookup_soup.findAll("a", {"class": "sic"})
                            numbers = [int(i) for i in lookup_tags[2].text.split() if i.isdigit()]
                            SIC_code = "".join(str(numbers)).replace("[", "").replace("]", "")

                            replace_dict[line["source"]] = SIC_code

                            line["source"] = SIC_code

                        except IndexError:
                            # unable to find SIC code through conventional means, and manual research showed that
                            # too much ambiguity was present to make a valid determination as to SIC code.
                            # 344 sources lost, which was deemed acceptable.
                            pass

                SIC.write(json.dumps(line) + "\n")

SIC.close()
