require 'gds-api-adapters'

namespace :content do
  desc 'Updates suggested related links for content from a JSON file'
  task :update_related_links_from_json, [:json_path] do |_, args|
    UPDATES_PER_BATCH = 20000

    @publishing_api = GdsApi::PublishingApiV2.new(
     ENV['PUBLISHING_API_URI'],
     bearer_token: ENV['PUBLISHING_API_BEARER_TOKEN']
    )

    puts 'Reading and parsing JSON'
    url = URI.parse(args[:json_path]) rescue false
    file =
      if url.is_a?(URI::HTTP) || url.is_a?(URI::HTTPS)
        url.open(&:read)
      else
        File.read(args[:json_path])
      end

    @failed_content_ids = []

    json = JSON.parse(file)
    source_content_id_groups = json.keys.each_slice(UPDATES_PER_BATCH)

    start_time = Time.now
    puts "Start updating content items, at #{start_time}"
    puts "Updating #{UPDATES_PER_BATCH} per batch"

    source_content_id_groups.each do |group|
      group.each do |source_content_id|
        update_content(source_content_id, json[source_content_id])
      end

      if group.length == UPDATES_PER_BATCH
        puts "Waiting for 30 minutes before starting next batch..."
        sleep 1800

        puts "Resuming ingestion of next batch at #{Time.now}"
      end
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
