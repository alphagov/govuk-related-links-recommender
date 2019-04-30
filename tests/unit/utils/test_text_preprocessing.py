import pytest

import numpy as np

from src.utils.text_preprocessing import *


def test_is_html():
    assert is_html('<h1>this is a heading</h1>') is True
    assert is_html(3) is False
    assert is_html('this is not html') is False
    assert is_html({'this': 'is', 'not': 'html'}) is False
    assert is_html(
        '<p> adsanlksa <a href="https://www.gov.uk/about/Pages/people/biographies/king>Mervyn King, Governor of the Bank of England</a></p>'
    ) is True


def test_extract_links_from_html():
    assert extract_links_from_html(
        '<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>'
    ) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']
    assert extract_links_from_html(
        '<p> adsanlksa <a href="https://www.gov.uk/about/Pages/people/biographies/king">Mervyn King, Governor of the Bank of England</a></p>'
    ) == ['/about/Pages/people/biographies/king']
    # test we don't pick up external links
    assert extract_links_from_html(
        '<p> adsanlksa <a href="https://www.bankofengland.co.uk/about/Pages/people/biographies/king">Mervyn King, Governor of the Bank of England</a></p>'
    ) == []
    # test we don't pick up government uploads beginning with /government/uploads/system/uploads/attachment_data/file/
    assert extract_links_from_html(
        '<h2 id=\"summary\">Summary:</h2>\n<p>During the ground roll after landing, the nose landing gear folded backwards and the nose and propeller struck the ground.  The damage to the nose gear was consistent with overload forces, but the pilot was unable to say how such loads had been generated.</p>\n\n<h3 id=\"download-report\">Download report:</h3>\n<p><a rel=\"external\" href=\"https://assets.digital.cabinet-office.gov.uk/media/56bc45e6e5274a0369000021/Vans_RV-9A_G-XSAM_02-16.pdf\">Vans RV-9A, G-XSAM 02-16</a></p>\n\n<h3 id=\"download-glossary-of-abbreviations\">Download glossary of abbreviations:</h3>\n<p><a href=\"https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/433812/Glossary_of_abbreviations.pdf\">Glossary of abbreviations</a></p>\n\n'
    ) == []


# TODO: add nested fields into tests, don't pass in links=[]
def test_extract_links_from_content_details():
    # test string
    assert extract_links_from_content_details(
        '<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>'
    ) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']
    # test dict
    assert extract_links_from_content_details({'content_type': 'text/govspeak',
     'content': 'Read the full decision in [embed:attachments:inline:d3e3f01b-020c-42d3-97b0-3bb0dfb96640].'}) == []
    assert extract_links_from_content_details({'content_type': 'text/html',
     'content': '<p>Read the full decision in <span class="attachment-inline"><a href="https://www.gov.uk/government/consultations/16-to-19-accountability-consultation">Mr G Burns and others v GEC Solutions Ltd T/a Green Energy Consulting: 2500762/2017 and others - Judgment with Reasons</a></span>.</p>'}
                                              ) == ['/government/consultations/16-to-19-accountability-consultation']

    # test list
    assert extract_links_from_content_details(
        [
            '<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>',
            ]) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']

    assert extract_links_from_content_details(
        ['<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>',
         np.nan,
         np.nan,
         np.nan,
         np.nan,
         True,
         '2015-conservative-government',
         '2015 Conservative government']) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']
    assert extract_links_from_content_details(
        []) == []
    # test list of dicts
    assert extract_links_from_content_details(
        [{'content_type': 'text/govspeak',
         'content': 'Read the full decision in [embed:attachments:inline:d3e3f01b-020c-42d3-97b0-3bb0dfb96640].'},
         {'content_type': 'text/html',
          'content': '<p>Read the full decision in <span class="attachment-inline"><a href="https://www.gov.uk/government/consultations/16-to-19-accountability-consultation">Mr G Burns and others v GEC Solutions Ltd T/a Green Energy Consulting: 2500762/2017 and others - Judgment with Reasons</a></span>.</p>'
          }]
    ) == ['/government/consultations/16-to-19-accountability-consultation']



def test_clean_page_path():
    assert clean_page_path('//guidance/animal-welfare#animal-welfare-during-transport') == "/guidance/animal-welfare"
    # test if there are multiple hashes, only chop off last part of split
    assert clean_page_path('//guidance/#animal-welfare#animal-welfare-during-transport') == "/guidance/#animal-welfare"
