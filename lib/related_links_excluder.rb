require 'json'

class RelatedLinksExcluder
  def self.apply_exclusions(publishing_api, related_links_json_file_extractor, exclusions_json_file_extractor)
    urls = exclusions_json_file_extractor.extracted_json
    base_paths = urls.map { |ri| URI.parse(ri).path }
    excluded_content_ids = publishing_api.lookup_content_ids(
      base_paths: base_paths
    ).values

    excluded_content_ids.each do |excluded_content_id|
      related_links_json_file_extractor.extracted_json.delete(excluded_content_id)
    end
  end
end
