require 'spec_helper'
require_relative '../../lib/related_links_updater'

require 'json'

describe RelatedLinksUpdater do
  context 'update_related_links' do
    let(:publishing_api) { double }
    let(:publishing_api_response) { double }
    let(:json_file_extractor) { double }

    before do
      extracted_json = JSON.parse('{
          "f593677f-a41f-4ca4-a169-5513a1250125": [
            "a98752a8-2b7c-4b49-896e-5eea396b267e",
            "3c97e19d-a1e5-4800-887d-9ae5d57ad92f",
            "4cc61dfa-2ee6-4263-a4a3-5c852afc8720",
            "de65ab20-c151-489f-afed-57151cb13895",
            "aa8fc376-65c0-4a14-8bc2-1a364f1fed72"
          ],
          "b43584db-0b4b-4d49-9a65-4d4ec42c9394": [
            "ae4c508b-c045-44be-9ccf-dde4555498b1",
            "eb771368-c26d-4519-a964-0769762b3700",
            "12238203-3a7d-400b-90fa-5ae873c43ecf",
            "5e393e14-7631-11e4-a3cb-005056011aef",
            "5e3940ed-7631-11e4-a3cb-005056011aef"
          ]
        }')

      allow(json_file_extractor).to receive(:get_batched_keys).and_return([extracted_json.keys])
      allow(json_file_extractor).to receive(:extracted_json).and_return(extracted_json)
      allow(publishing_api).to receive(:patch_links).and_return(publishing_api_response)
    end

    it 'should call publishing API to update related links and receive success responses' do
      allow(publishing_api_response).to receive(:code).and_return(200)

      related_links_updater = RelatedLinksUpdater.new(publishing_api, json_file_extractor, 10)
      related_links_updater.update_related_links

      expect(related_links_updater.failed_content_ids).to eq([])
    end

    it 'should call publishing API to update related links and receive failed content ids' do
      allow(publishing_api_response).to receive(:code).and_return(500)

      related_links_updater = RelatedLinksUpdater.new(publishing_api, json_file_extractor, 10)
      related_links_updater.update_related_links

      expect(related_links_updater.failed_content_ids).to eq(['f593677f-a41f-4ca4-a169-5513a1250125', 'b43584db-0b4b-4d49-9a65-4d4ec42c9394'])
    end

    it 'should sleep when ingesting links where multiple batches exist' do
      allow(publishing_api_response).to receive(:code).and_return(200)
      allow(Kernel).to receive(:sleep)

      related_links_updater = RelatedLinksUpdater.new(publishing_api, json_file_extractor, 1)
      related_links_updater.update_related_links

      expect(related_links_updater.failed_content_ids).to eq([])
      expect(Kernel).to receive(:sleep).with(1200).once
    end
  end
end
