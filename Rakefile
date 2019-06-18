require 'gds-api-adapters'

namespace :content do
  desc 'Updates suggested related links for content from a JSON file'
  task :update_related_links_from_json, [:json_path] do |_, args|
    puts 'Reading and parsing data...'

    @publishing_api = GdsApi::PublishingApiV2.new(
     ENV['PUBLISHING_API_URI'],
     bearer_token: ENV['PUBLISHING_API_BEARER_TOKEN']
    )

    url = URI.parse(args[:json_path]) rescue false
    file =
      if url.is_a?(URI::HTTP) || url.is_a?(URI::HTTPS)
        url.open(&:read)
      else
        File.read(args[:json_path])
      end

    json = JSON.parse(file)

    @failed_content_ids = []

    start_time = Time.now
    puts "Start updating content items, at #{start_time}"

    json.each_pair do |source_content_id, related_content_ids|
      update_content(source_content_id, related_content_ids)
    end

    end_time = Time.now
    elapsed_time = end_time - start_time

    puts "Total elapsed time: #{elapsed_time}s"
    puts "Failed content ids: #{@failed_content_ids}"
  end

  def update_content(source_content_id, related_content_ids)
    response = @publishing_api.patch_links(
      source_content_id,
      links: {
        suggested_ordered_related_items: related_content_ids
      },
      bulk_publishing: true
    )

    if response.code == 200
      puts "Updated related links for content #{source_content_id}"
    else
      @failed_content_ids << source_content_id
      STDERR.puts "Failed to update content id #{source_content_id} - response status #{response.code}"
    end
  end
end
