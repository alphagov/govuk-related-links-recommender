require 'spec_helper'
require_relative '../../lib/json_file_extractor'
require_relative '../../lib/related_links_excluder'

require 'json'

describe RelatedLinksExcluder do
  context 'apply_exclusions' do
    let(:related_links_json_file_extractor) { double }
    let(:exclusions_json_file_extractor) { double }
    let(:publishing_api) { double }

    before do
      exclusions_json = JSON.parse('[
        "f593677f-a41f-4ca4-a169-5513a1250125",
        "eb771368-c26d-4519-a964-0769762b3700"
      ]')

      expected_base_paths = {:base_paths=>["/minstry-of-magic", "/guidance/use-of-spells-in-school-grounds", "/apply-for-a-magic-permit"]}

      allow(publishing_api).to receive(:lookup_content_ids).with(expected_base_paths).and_return({
        "/minstry-of-magic": "b43584db-0b4b-4d49-9a65-4d4ec42c9394",
        "/guidance/use-of-spells-in-school-grounds": "eb771368-c26d-4519-a964-0769762b3700",
        "/apply-for-a-magic-permit": "5f4da0ff-7631-11e4-a3cb-005056011aef"
      })
    end

    it 'should apply exclusions successfully' do
      expected_filtered_json = JSON.parse('{
          "f593677f-a41f-4ca4-a169-5513a1250125": [
            "a98752a8-2b7c-4b49-896e-5eea396b267e",
            "3c97e19d-a1e5-4800-887d-9ae5d57ad92f",
            "4cc61dfa-2ee6-4263-a4a3-5c852afc8720",
            "de65ab20-c151-489f-afed-57151cb13895",
            "aa8fc376-65c0-4a14-8bc2-1a364f1fed72"
          ],
          "5f4d997d-7631-11e4-a3cb-005056011aef": [
            "5f4da0ff-7631-11e4-a3cb-005056011aef"
          ]
        }')

      related_links_json_file_extractor = JsonFileExtractor.new('./spec/fixtures/sample_extracted_links.json')
      exclusions_json_file_extractor = JsonFileExtractor.new('./spec/fixtures/sample_exclusions.json')

      RelatedLinksExcluder.apply_exclusions(publishing_api, related_links_json_file_extractor, exclusions_json_file_extractor)

      expect(related_links_json_file_extractor.extracted_json.size).to eq(2)
      expect(related_links_json_file_extractor.extracted_json).to eq(expected_filtered_json)
    end
  end
end
