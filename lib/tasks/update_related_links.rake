require 'gds_api/publishing_api'

namespace :content do
  desc 'Updates suggested related links for content from a JSON file'
  task :update_related_links_from_json, [:json_path,:exclusions_path] do |_, args|
    UPDATES_PER_BATCH = 20000

    publishing_api = GdsApi::PublishingApi.new(
      ENV['PUBLISHING_API_URI'],
      bearer_token: ENV['PUBLISHING_API_BEARER_TOKEN']
    )

    puts 'Reading and parsing JSON'
    json_file_extractor = JsonFileExtractor.new(args[:json_path])

    puts 'Reading and parsing exclusions JSON'
    exclusions_file_extractor = JsonFileExtractor.new(args[:exclusions_path])

    # Apply exclusions before batching
    puts 'Apply content item exclusions'
    puts "Initial update count: #{json_file_extractor.extracted_json.length}"

    RelatedLinksExcluder.apply_exclusions(publishing_api, json_file_extractor, exclusions_file_extractor)

    puts "Finished applying content item exclusions"
    puts "Filtered update count: #{json_file_extractor.extracted_json.length}"

    links_updater = RelatedLinksUpdater.new(publishing_api, json_file_extractor, UPDATES_PER_BATCH)
    links_updater.update_related_links
  end
end
