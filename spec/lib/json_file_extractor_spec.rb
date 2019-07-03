require 'spec_helper'
require_relative '../../lib/json_file_extractor'

require 'json'

describe JsonFileExtractor do
  describe 'extract JSON' do
    it 'should return empty hash when input JSON file does not exist' do
      file_extractor = JsonFileExtractor.new('./this/does/not/exist.json')

      expect(file_extractor.extracted_json).to eq({})
    end

    it 'should return the extracted json' do
      expected_json = '{
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
      }'

      file_extractor = JsonFileExtractor.new('./spec/fixtures/sample_extracted_links.json')

      expect(file_extractor.extracted_json).to eq(JSON.parse(expected_json))
    end
  end

  describe 'get batched keys' do
    it 'should return keys without batching when number of keys is less than or equal to batch_size' do
      expected_batched_keys = ['f593677f-a41f-4ca4-a169-5513a1250125', 'b43584db-0b4b-4d49-9a65-4d4ec42c9394']

      file_extractor = JsonFileExtractor.new('./spec/fixtures/sample_extracted_links.json')

      batched_keys_enumerator = file_extractor.get_batched_keys(5)

      expect(batched_keys_enumerator.size).to eq(1)
      expect(batched_keys_enumerator.next).to eq(expected_batched_keys)
    end


    it 'should return keys batched by the given batch_size when number of keys is greater than batch_size' do
      file_extractor = JsonFileExtractor.new('./spec/fixtures/sample_extracted_links.json')

      batched_keys_enumerator = file_extractor.get_batched_keys(1)

      expect(batched_keys_enumerator.size).to eq(2)
      expect(batched_keys_enumerator.next).to eq(['f593677f-a41f-4ca4-a169-5513a1250125'])
      expect(batched_keys_enumerator.next).to eq(['b43584db-0b4b-4d49-9a65-4d4ec42c9394'])
    end
  end
end
