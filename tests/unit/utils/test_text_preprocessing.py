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
        '<p> adsanlksa <a href="/government/uploads/system/uploads/attachment_data/file/about/Pages/people/biographies/king">Mervyn King, Governor of the Bank of England</a></p>'
    ) == []


def test_extract_links_from_content_details():
    # test string
    assert extract_links_from_content_details(
        '<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>',
        links=[]) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']
    # test dict
    # test list
    assert extract_links_from_content_details(
        [
            '<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>',
            ], links=[]
    ) == ['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare']

    assert len(extract_links_from_content_details(
        ['<p>You’re responsible for any animals you keep on your farm and you must have enough staff with the training, knowledge and skills to look after them properly. This guide explains your responsibilities and helps you follow the Welfare of Farmed Animals Regulations 2007 and related laws.</p>\n\n<p>You and your staff must also have read, understand and have access to the welfare codes of recommendations for the animals you keep. Read the:</p>\n\n<ul>\n  <li>\n<a href="https://www.gov.uk/government/publications/poultry-on-farm-welfare">codes of recommendations and animal welfare guides</a> for laying hens, broiler (meat) chickens, ducks and turkeys</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle">cattle code of recommendations and animal welfare guide</a> (beef cattle and dairy cows)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs">pigs code of recommendations and animal welfare guide</a> (including breeding sows)</li>\n  <li>specific <a href="https://www.gov.uk/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep">sheep and goats codes of recommendations and animal welfare guides</a> (including milk sheep)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/deer-on-farm-welfare">deer code of recommendations and animal welfare guide</a> (farmed deer only)</li>\n  <li>\n<a href="https://www.gov.uk/government/publications/rabbits-on-farm-welfare">rabbits code of recommendations and animal welfare guide</a> (farmed rabbits only)</li>\n</ul>\n\n<p>The welfare codes of recommendations include both legal requirements and good practice to help you follow the law.</p>\n\n<h2 id="your-responsibilities">Your responsibilities</h2>\n\n<p>It’s against the law to neglect or be cruel to a farm animal. If you’re responsible for an animal you must make sure that you care for it properly.</p>',
         np.nan,
         np.nan,
         np.nan,
         np.nan,
         True,
         '2015-conservative-government',
         '2015 Conservative government'], links=[])
    ) == len(['/government/publications/poultry-on-farm-welfare',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-cattle',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-pigs',
          '/government/publications/code-of-recommendations-for-the-welfare-of-livestock-sheep',
          '/government/publications/deer-on-farm-welfare',
          '/government/publications/rabbits-on-farm-welfare'])


# def test_clean_page_path():
