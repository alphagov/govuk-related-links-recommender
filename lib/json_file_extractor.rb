class JsonFileExtractor
  attr_reader  :extracted_json

  def initialize(json_path)
    extract_json(json_path)
  end

  def get_batched_keys(batch_size)
    extracted_json.keys.each_slice(batch_size)
  end

  private
  def extract_json(json_path)
    url = URI.parse(json_path) rescue false
    file =
      if url.is_a?(URI::HTTP) || url.is_a?(URI::HTTPS)
        url.open(&:read)
      else
        File.read(json_path)
      end

    @extracted_json = JSON.parse(file)
  end
end