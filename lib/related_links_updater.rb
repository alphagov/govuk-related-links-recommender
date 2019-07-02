require 'progressbar'

class RelatedLinksUpdater
  def initialize(publishing_api, json_file_extractor, updates_per_batch, between_batch_wait_time_seconds = 1200)
    @publishing_api = publishing_api
    @json_file_extractor = json_file_extractor
    @updates_per_batch = updates_per_batch
    @between_batch_wait_time_seconds = between_batch_wait_time_seconds
  end

  def update_related_links
    @failed_content_ids = []

    puts "Wait time between batches: #{between_batch_wait_time_seconds}s"
    puts "Updates per batch: #{updates_per_batch}"

    start_time = Time.now
    puts "Start updating content items, at #{start_time}"
    puts "Updating #{UPDATES_PER_BATCH} per batch"

    source_content_id_groups = json_file_extractor.get_batched_keys(UPDATES_PER_BATCH)
    source_content_id_groups.each_with_index do |group, batch_index|
      puts "Starting batch ##{batch_index}"

      progress_bar = ProgressBar.create(:throttle_rate => 0.1, :format => "Processed %c of %C %B", :total => group.length)

      group.each_with_index do |source_content_id, item_index|
        update_content(source_content_id, json_file_extractor.extracted_json[source_content_id], item_index)
        progress_bar.increment
      end

      puts "Finished batch ##{batch_index}"

      if group.length == UPDATES_PER_BATCH
        puts "Waiting for 20 minutes before starting next batch..."
        sleep between_batch_wait_time_seconds

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
  attr_reader :json_file_extractor, :updates_per_batch, :between_batch_wait_time_seconds, :publishing_api, :failed_content_ids

  def update_content(source_content_id, related_content_ids, item_index)
    response = publishing_api.patch_links(
      source_content_id,
      links: {
        suggested_ordered_related_items: related_content_ids
      },
      bulk_publishing: true
    )

    if response.code != 200
      failed_content_ids << source_content_id
      STDERR.puts "Failed to update content id #{source_content_id} - response status #{response.code}"
    end
  end
end
