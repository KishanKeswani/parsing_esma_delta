import logging
import requests
import subprocess
import csv
import xml
from zipfile import ZipFile
from io import BytesIO
from xml.etree import ElementTree as ET
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('parsing_logs.txt', mode='a', encoding=None,
                           delay=False)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


class parseData:
    def parse_second_xml(self, download_url):
        """
        Parses the delta file into csv as final output file
        """
        response = requests.get(download_url, stream=True)
        if response.status_code == 200:
            logger.info("Fetched data from downloaded url")
            file = ZipFile(BytesIO(response.content))
            final_output_list = []
            temp_dict = {}
            with file.open('DLTINS_20210117_01of01.xml') as xml_data:
                tree = ET.fromstring(xml_data.read())
                for child in tree.iter('*'):
                    if child.tag.split('}')[1] == 'FinInstrmGnlAttrbts':
                        for ch in child.iter('*'):
                            val = ch.text
                            key = ch.tag.split('}')[1]
                            if key not in ["FinInstrmGnlAttrbts", "ShrtNm"]:
                                temp_dict["FinInstrmGnlAttrbts." + key] = val
                    if child.tag.split('}')[1] == 'Issr':
                        key = 'Issr'
                        val = child.text
                        temp_dict[key] = val
                    if temp_dict and 'Issr' in temp_dict:
                        final_output_list.append(temp_dict)
                        temp_dict = {}
            logger.info("Data Format : {0}".format(final_output_list[0]))
            with open('output_data.csv', 'w', encoding='utf8',
                      newline='') as output_file:
                keys = final_output_list[0].keys()
                fc = csv.DictWriter(output_file, fieldnames=keys)
                fc.writeheader()
                fc.writerows(final_output_list)
            logger.info("Successfully parsed downloaded file into csv file")
            logger.info("Parsed Filename : output_data.csv")
        elif response.status_code == 404:
            logger.info('Downloaded Resource Not Found.')

    def parse_first_xml(self):
        """
        Parses xml provided in the git url and gets delta file url
        """
        url = ('https://registers.esma.europa.eu/solr/esma_registers_'
               'firds_files/select?q=*&fq=publication_date:%5B2021-01-17'
               'T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true'
               '&start=0&rows=100')
        response = requests.get(url)
        logger.info("Starting URL : {0}".format(url))
        if response.status_code == 200:
            tree = ET.fromstring(response.content)
            download_link = None
            for child in tree.iter('*'):
                if 'name' in child.attrib.keys():
                    attr = child.attrib['name']
                    if attr == 'download_link':
                        download_link = child.text

                    if attr == 'file_type' and child.text == 'DLTINS':
                        break
            self.parse_second_xml(download_link)

        elif response.status_code == 404:
            logger.info('Downloading Url Not Found.')

if __name__ == '__main__':
    """
    This script is used to download and parse delta files from esma website

    Run pydoc using below command :

        python3 -m pydoc parsing_script

    """
    logger.info('Script started')
    try:
        parsing_object = parseData()
        parsing_object.parse_first_xml()
    except Exception as e:
        logger.info("Got Exception in parsing data :{0}".format(e))
    else:
        logger.info("Script successfully completed")
    finally:
        logger.info('Uploading outfile to S3')
        inpath = "output_data.csv"
        outpath = "s3://steeleye_bucket/"
        subprocess.run(["aws", "s3", "cp", inpath, outpath])
        logger.info('File uploaded to s3 . Script finished successfully')
