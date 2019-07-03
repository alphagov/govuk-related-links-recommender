require 'json'

class JsonFileExtractor
  attr_reader  :extracted_json

  def initialize(json_path)
    @extracted_json = extract_json(json_path)
  end

  def get_batched_keys(batch_size)
    extracted_json.keys.each_slice(batch_size)
  end

  private

  def extract_json(json_path)
    begin
      file = File.read(json_path)
      JSON.parse(file)
    rescue
      {}
    end
  end
end
