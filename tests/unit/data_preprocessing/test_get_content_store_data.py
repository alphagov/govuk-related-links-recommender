import pytest

import json
import pandas as pd
import pandas.testing as pd_testing

from src.data_preprocessing.get_content_store_data import *


# TODO: look into creating our own fixtures, with a module scope instead of function, instead of pytest-mongodb

def test_get_all_links_df(mongodb):
    # print(get_all_links_df(mongodb.content_store_data_sample).head())
    with open('/Users/ellieking/Documents/govuk-related-links-recommender/tests/unit/fixtures/base_path_content_id_mapping.json', 'r') as infile:
        mapping = json.load(infile)
    pd_testing.assert_frame_equal(
        get_all_links_df(mongodb.content_store_data_sample, mapping).sort_values(
            by=['source_content_id', 'destination_content_id']).reset_index(drop=True),
        pd.read_csv('tests/unit/fixtures/all_links_test_sample.csv').sort_values(
            by=['source_content_id', 'destination_content_id']).reset_index(drop=True))

def test_get_excluded_document_types():
    assert get_excluded_document_types() == ['about',
                                             'about_our_services',
                                             'access_and_opening',
                                             'business_support_finder',
                                             'coming_soon',
                                             'complaints_procedure',
                                             'completed_transaction',
                                             'contact',
                                             'corporate_report',
                                             'dfid_research_output',
                                             'equality_and_diversity',
                                             'field_of_operation',
                                             'finder',
                                             'finder_email_signup',
                                             'gone',
                                             'help_page',
                                             'hmrc_manual_section',
                                             'homepage',
                                             'html_publication',
                                             'licence_finder',
                                             'mainstream_browse_page',
                                             'manual_section',
                                             'media_enquiries',
                                             'membership',
                                             'ministerial_role',
                                             'need',
                                             'organisation',
                                             'our_energy_use',
                                             'our_governance',
                                             'person',
                                             'personal_information_charter',
                                             'placeholder_ministerial_role',
                                             'placeholder_person',
                                             'placeholder_policy_area',
                                             'placeholder_topical_event',
                                             'placeholder_world_location_news_page',
                                             'policy_area',
                                             'publication_scheme',
                                             'redirect',
                                             'search',
                                             'service_manual_guide',
                                             'service_manual_homepage',
                                             'service_manual_service_standard',
                                             'service_manual_service_toolkit',
                                             'service_manual_topic',
                                             'service_standard_report',
                                             'services_and_information',
                                             'social_media_use',
                                             'special_route',
                                             'staff_update',
                                             'taxon',
                                             'topic',
                                             'topical_event',
                                             'topical_event_about_page',
                                             'travel_advice',
                                             'travel_advice_index',
                                             'working_group',
                                             'world_location',
                                             'worldwide_organisation']

def test_get_links(mongodb):
    assert sorted(get_links(mongodb.content_store_data_sample, 'related'), key=lambda k: k['_id']) == sorted([
        {'_id': '/agency-workers-your-rights',
         'expanded_links': {'ordered_related_items': [{'base_path': '/employment-contracts-and-conditions',
                                                       'content_id': '65c41235-b98f-4991-a1cb-fb4b9d1be105'},
                                                      {'base_path': '/fixed-term-contracts',
                                                       'content_id': '9e6aa652-9939-49c3-a400-245662a31e05'}]},
         'content_id': '6395a828-671d-4e0c-bc0b-719e3da51ca8'},
        {'_id': '/order-copy-birth-death-marriage-certificate',
         'expanded_links': {'ordered_related_items': [{'base_path': '/register-offices',
                                                       'content_id': '5608af75-4935-43d1-996f-09fbebde5ecd'},
                                                      {'base_path': '/search-local-archives',
                                                       'content_id': 'b02557ce-b931-4c1c-aa5f-ace642f45759'}]},
         'content_id': '17a26e6f-1f1d-4585-a9f6-433272730101'},
        {'_id': '/hypoglycaemia-and-driving',
         'expanded_links': {'ordered_related_items': [{'base_path': '/giving-up-your-driving-licence',
                                                       'content_id': 'b68905c5-7ca4-4d3f-8fb2-f4544365e2ed'},
                                                      {'base_path': '/health-conditions-and-driving',
                                                       'content_id': '250a1bf5-ddd3-4d68-820b-4c1efe087767'},
                                                      {'base_path': '/driving-medical-conditions',
                                                       'content_id': '21574365-214c-49e1-a28a-bcec5f232ef9'},
                                                      {'base_path': '/reapply-driving-licence-medical-condition',
                                                       'content_id': 'a7d4b9a2-b0c5-4841-937e-b264e9845d28'}]},
         'content_id': '1b726b4d-f215-4922-a5c5-622bfac8db29'},
        {'_id': '/tax-right-retire-abroad-return-to-uk',
         'expanded_links': {'ordered_related_items': [{'base_path': '/state-pension-if-you-retire-abroad',
                                                       'content_id': '67e60d9c-6479-49f1-be15-d895c2796a94'},
                                                      {'base_path': '/national-insurance-if-you-go-abroad',
                                                       'content_id': '833a9ae0-7045-4209-b817-def774f3dfbf'},
                                                      {'base_path': '/moving-or-retiring-abroad',
                                                       'content_id': '47a700b1-fa6b-4950-b491-d25798c7711f'},
                                                      {'base_path': '/tax-credits-if-moving-country-or-travelling',
                                                       'content_id': '169f034f-ce78-41a6-98aa-7aafbd862067'},
                                                      {'base_path': '/working-abroad',
                                                       'content_id': '12806ea7-f02c-4a2f-acd4-98df5e38ed64'},
                                                      {'base_path': '/tax-uk-income-live-abroad',
                                                       'content_id': '1e165fdc-0139-434a-ab47-7a6c13a38036'},
                                                      {'base_path': '/claim-benefits-abroad',
                                                       'content_id': '22f341ca-82c8-4637-9dc9-e83619a5255a'}]},
         'content_id': '090fa2f4-d596-47d4-8bf7-7dceec4ac4f1'},
        {'_id': '/legal-aid',
         'expanded_links': {'ordered_related_items': [{'base_path': '/court-fees-what-they-are',
                                                       'content_id': 'c1b3a2fe-9bf5-44ee-a76c-7d34d4880023'},
                                                      {'base_path': '/courts',
                                                       'content_id': '04780a32-b359-48e2-b103-79862348dc62'},
                                                      {'base_path': '/check-legal-aid',
                                                       'content_id': '2912cb9f-fe86-4825-8dc8-88307f3b92ad'},
                                                      {'base_path': '/civil-legal-advice',
                                                       'content_id': '9f16b522-0321-4e6c-9185-e233306ed328'}]},
         'content_id': '84f68c01-c5f3-4d20-9b31-d46fec04498c'}], key=lambda k: k['_id'])
# def test_convert_link_list_to_df():

# def test_get_base_path_to_content_id_mapping():

# def test_get_page_text_df():

# def test_reshape_df_explode_list_column():

# def test_extract_embedded_links_df():

# def get_all_links_df():


