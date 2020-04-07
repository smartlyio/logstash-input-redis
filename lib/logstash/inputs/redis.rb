# encoding: utf-8
require_relative '../plugin_mixins/redis_connection'
require "logstash/namespace"
require "logstash/inputs/base"
require "logstash/inputs/threadable"
require 'redis'
require 'concurrent'
require 'concurrent/executors'

# This input will read events from a Redis instance; it supports both Redis channels and lists.
# The list command (BLPOP) used by Logstash is supported in Redis v1.3.1+, and
# the channel commands used by Logstash are found in Redis v1.3.8+.
# While you may be able to make these Redis versions work, the best performance
# and stability will be found in more recent stable versions.  Versions 2.6.0+
# are recommended.
#
# For more information about Redis, see <http://redis.io/>
#
# `batch_count` note: If you use the `batch_count` setting, you *must* use a Redis version 2.6.0 or
# newer. Anything older does not support the operations used by batching.
#
module LogStash module Inputs class Redis < LogStash::Inputs::Threadable

  include ::LogStash::PluginMixins::RedisConnection

  BATCH_EMPTY_SLEEP = 0.25

  config_name "redis"

  default :codec, "json"

  # The name of a Redis list or channel.
  config :key, :validate => :string, :required => true

  # Specify either list or channel.  If `data_type` is `list`, then we will BLPOP the
  # key. If `data_type` is `pattern_list`, then we will spawn a number of worker
  # threads that will LPOP from keys matching that pattern. If `data_type` is
  # `channel`, then we will SUBSCRIBE to the key. If `data_type` is `pattern_channel`,
  # then we will PSUBSCRIBE to the key.
  config :data_type, :validate => [ "list", "pattern_list", "channel", "pattern_channel" ], :required => true

  # The number of events to return from Redis using EVAL.
  config :batch_count, :validate => :number, :default => 125

  # Maximum number of worker threads to spawn when using `data_type` `pattern_list`.
  config :pattern_list_threads, :validate => :number, :default => 20

  # Maximum number of items for a single worker thread to process when `data_type` is `pattern_list`.
  # After the list is empty or this number of items have been processed, the thread will exit and a
  # new one will be started if there are non-empty lists matching the pattern without a consumer.
  config :pattern_list_max_items, :validate => :number, :default => 1000

  # Time to sleep in main loop after checking if more threads can/need to be spawned.
  # Applies to `data_type` is `pattern_list`
  config :pattern_list_threadpool_sleep, :validate => :number, :default => 0.2

  public
  def init_threadpool
    @threadpool ||= Concurrent::ThreadPoolExecutor.new(
        min_threads: @pattern_list_threads,
        max_threads: @pattern_list_threads,
        max_queue: 2 * @pattern_list_threads
    )
    @current_workers ||= Concurrent::Set.new
  end

  def register
    @redis_url = @path.nil? ? "redis://#{@password}@#{@host}:#{@port}/#{@db}" : "#{@password}@#{@path}/#{@db}"

    @redis_builder ||= method(:internal_redis_builder)

    # just switch on data_type once
    if @data_type == 'list' || @data_type == 'dummy'
      @run_method = method(:list_runner)
      @stop_method = method(:list_stop)
    elsif @data_type == 'pattern_list'
      @run_method = method(:pattern_list_runner)
      @stop_method = method(:pattern_list_stop)
    elsif @data_type == 'channel'
      @run_method = method(:channel_runner)
      @stop_method = method(:subscribe_stop)
    elsif @data_type == 'pattern_channel'
      @run_method = method(:pattern_channel_runner)
      @stop_method = method(:subscribe_stop)
    end

    @batched = is_list_type? && batched?

    @identity = "#{@redis_url} #{@data_type}:#{@key}"
    @logger.info("Registering Redis", :identity => @identity)
  end # def register

  def run(output_queue)
    @run_method.call(output_queue)
  rescue LogStash::ShutdownSignal
    # ignore and quit
  end # def run

  def stop
    @stop_method.call
  end

  # private methods -----------------------------
  private

  def batched?
    @batch_count > 1
  end

  # private
  def is_list_type?
    @data_type == 'list' || @data_type == 'pattern_list'
  end

  # private
  def queue_event(msg, output_queue, channel=nil)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        event.set("[@metadata][redis_channel]", channel) if !channel.nil?
        output_queue << event
      end
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e, :backtrace => e.backtrace);
    end
  end

  def list_stop
    reset_redis
  end

  # private
  def list_runner(output_queue)
    @list_method = @batched ? method(:list_batch_listener) : method(:list_single_listener)
    while !stop?
      redis_runner(@batched) do
        @list_method.call(@redis, output_queue)
      end
    end
  end

  #private
  def reset_threadpool
    return if @threadpool.nil?
    @threadpool.shutdown
    @threadpool.wait_for_termination
    @threadpool = nil
  end

  # private
  def pattern_list_stop
    reset_redis
    reset_threadpool
  end

  # private
  def pattern_list_process_item(redis, output_queue, key)
    if stop?
      @logger.debug("Breaking from thread #{key} as it was requested to stop")
      return false
    end
    value = redis.lpop(key)
    return false if value.nil?
    queue_event(value, output_queue)
    true
  end

  # private
  def pattern_list_single_processor(redis, output_queue, key)
    (0...@pattern_list_max_items).each do
      break unless pattern_list_process_item(redis, output_queue, key)
    end
  end

  # private
  def pattern_list_batch_processor(redis, output_queue, key)
    items_left = @pattern_list_max_items
    while items_left > 0
      limit = [items_left, @batch_count].min
      processed = process_batch(redis, output_queue, key, limit, 0)
      if processed.zero? || processed < limit
        return
      end
      items_left -= processed
    end
  end

  # private
  def pattern_list_worker_consume(output_queue, key)
    begin
      redis ||= connect(@batched)
      @pattern_list_processor.call(redis, output_queue, key)
    rescue ::Redis::BaseError => e
      @logger.warn("Redis connection problem in thread for key #{key}. Sleeping a while before exiting thread.", :exception => e)
      sleep 1
      return
    ensure
      redis.quit rescue nil
    end
  end

  # private
  def threadpool_capacity?
    @threadpool.remaining_capacity > 0
  end

  # private
  def pattern_list_launch_worker(output_queue, key)
    @current_workers.add(key)
    @threadpool.post do
      begin
        pattern_list_worker_consume(output_queue, key)
      ensure
        @current_workers.delete(key)
      end
    end
  end

  # private
  def pattern_list_ensure_workers(output_queue)
    return unless threadpool_capacity?
    redis_runner(@batched) do
      @redis.keys(@key).shuffle.each do |key|
        next if @current_workers.include?(key)
        pattern_list_launch_worker(output_queue, key)
        break unless threadpool_capacity?
      end
    end
  end

  # private
  def pattern_list_runner(output_queue)
    @pattern_list_processor = @batched ? method(:pattern_list_batch_processor) : method(:pattern_list_single_processor)
    while !stop?
      init_threadpool if @threadpool.nil?
      pattern_list_ensure_workers(output_queue)
      sleep(@pattern_list_threadpool_sleep)
    end
  end

  def process_batch(redis, output_queue, key, batch_size, sleep_time)
    begin
      results = redis.evalsha(@redis_script_sha, [key], [batch_size-1])
      results.each do |item|
        queue_event(item, output_queue)
      end
      sleep sleep_time if results.size.zero? && sleep_time > 0
      results.size

      # Below is a commented-out implementation of 'batch fetch'
      # using pipelined LPOP calls. This in practice has been observed to
      # perform exactly the same in terms of event throughput as
      # the evalsha method. Given that the EVALSHA implementation uses
      # one call to Redis instead of N (where N == @batch_count) calls,
      # I decided to go with the 'evalsha' method of fetching N items
      # from Redis in bulk.
      #redis.pipelined do
        #error, item = redis.lpop(@key)
        #(@batch_count-1).times { redis.lpop(@key) }
      #end.each do |item|
        #queue_event(item, output_queue) if item
      #end
      # --- End commented out implementation of 'batch fetch'
      # further to the above, the LUA script now uses lrange and trim
      # which should further improve the efficiency of the script
    rescue ::Redis::CommandError => e
      if e.to_s =~ /NOSCRIPT/ then
        @logger.warn("Redis may have been restarted, reloading Redis batch EVAL script", :exception => e)
        load_batch_script(redis)
        retry
      else
        raise e
      end
    end
  end

  def list_batch_listener(redis, output_queue)
    process_batch(redis, output_queue, @key, @batch_count, BATCH_EMPTY_SLEEP)
  end

  def list_single_listener(redis, output_queue)
    item = redis.blpop(@key, 0, :timeout => 1)
    return unless item # from timeout or other conditions

    # blpop returns the 'key' read from as well as the item result
    # we only care about the result (2nd item in the list).
    queue_event(item.last, output_queue)
  end

  # private
  def subscribe_stop
    return if @redis.nil? || !@redis.connected?
    # if its a SubscribedClient then:
    # it does not have a disconnect method (yet)
    if @redis.client.is_a?(::Redis::SubscribedClient)
      if @data_type == 'pattern_channel'
        @redis.client.punsubscribe
      else
        @redis.client.unsubscribe
      end
    else
      @redis.client.disconnect
    end
    @redis = nil
  end

  # private
  def channel_runner(output_queue)
    redis_runner do
      channel_listener(output_queue)
    end
  end

  # private
  def channel_listener(output_queue)
    @redis.subscribe(@key) do |on|
      on.subscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.message do |channel, message|
        queue_event(message, output_queue, channel)
      end

      on.unsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

  def pattern_channel_runner(output_queue)
    redis_runner do
      pattern_channel_listener(output_queue)
    end
  end

  # private
  def pattern_channel_listener(output_queue)
    @redis.psubscribe @key do |on|
      on.psubscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.pmessage do |pattern, channel, message|
        queue_event(message, output_queue, channel)
      end

      on.punsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

# end

end end end # Redis Inputs  LogStash
