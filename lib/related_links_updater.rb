require 'progressbar'

class RelatedLinksUpdater
  attr_reader :failed_content_ids

  def initialize(publishing_api, json_file_extractor, updates_per_batch, between_batch_wait_time_seconds = 1200)
    @publishing_api = publishing_api
    @json_file_extractor = json_file_extractor
    @updates_per_batch = updates_per_batch
    @between_batch_wait_time_seconds = between_batch_wait_time_seconds
    @failed_content_ids = []
  end

  def update_related_links
    puts "Wait time between batches: #{between_batch_wait_time_seconds}s"
    puts "Updates per batch: #{updates_per_batch}"

    start_time = Time.now
    puts "Start updating content items, at #{start_time}"
    puts "Updating #{updates_per_batch} per batch"

    source_content_id_groups = json_file_extractor.get_batched_keys(updates_per_batch)
    last_group_index = source_content_id_groups.count - 1
    source_content_id_groups.each_with_index do |group, batch_index|
      puts "Starting batch ##{batch_index}"

      progress_bar = ProgressBar.create(:throttle_rate => 0.1, :format => "Processed %c of %C %B", :total => group.length)

      group.each do |source_content_id|
        update_content(source_content_id, json_file_extractor.extracted_json[source_content_id])
        progress_bar.increment
      end

      puts "Finished batch ##{batch_index}"

      if batch_index < last_group_index
        wait_time = "#{(between_batch_wait_time_seconds / 60).ceil} minutes #{between_batch_wait_time_seconds % 60} seconds"
        puts "Waiting for #{wait_time} before starting next batch..."
        wait between_batch_wait_time_seconds

        puts "Resuming ingestion of next batch at #{Time.now}"
      else
        puts "All links ingested"
      end
    end

    end_time = Time.now
    elapsed_time = end_time - start_time

    puts "Total elapsed time: #{elapsed_time}s"
    puts "Failed content ids: #{@failed_content_ids}"

  end

private
  attr_reader :json_file_extractor, :updates_per_batch, :between_batch_wait_time_seconds, :publishing_api

  def update_content(source_content_id, related_content_ids)
    begin
      response = publishing_api.patch_links(
        source_content_id,
        links: {
          suggested_ordered_related_items: related_content_ids
        },
        bulk_publishing: true
      )

      if response.code != 200
        add_to_failed_updates(source_content_id, "Failed with status #{response.code}")
      end

    rescue GdsApi::HTTPBadGateway, GdsApi::HTTPUnavailable, GdsApi::HTTPGatewayTimeout, GdsApi::TimedOutException => e
      add_to_failed_updates(source_content_id, e)
    end

  end

  def wait(seconds)
    sleep seconds
  end

  def add_to_failed_updates(source_content_id, e)
    failed_content_ids << source_content_id
    STDERR.puts "Failed to update content id #{source_content_id} - exception #{e}"
  end
end
