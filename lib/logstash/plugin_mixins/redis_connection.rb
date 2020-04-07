# encoding: utf-8
require "logstash/namespace"

module LogStash module PluginMixins module RedisConnection
  def self.included(base)
    base.extend(self)
    base.setup_redis_connection_config
  end

  public

  def setup_redis_connection_config
    # The hostname of your Redis server.
    config :host, :validate => :string, :default => "127.0.0.1"

    # The port to connect on.
    config :port, :validate => :number, :default => 6379

    # SSL
    config :ssl, :validate => :boolean, :default => false

    # The unix socket path to connect on. Will override host and port if defined.
    # There is no unix socket path by default.
    config :path, :validate => :string

    # The Redis database number.
    config :db, :validate => :number, :default => 0

    # Initial connection timeout in seconds.
    config :timeout, :validate => :number, :default => 5

    # Password to authenticate with. There is no authentication by default.
    config :password, :validate => :password

    # Redefined Redis commands to be passed to the Redis client.
    config :command_map, :validate => :hash, :default => {}
  end

  # public API
  # use to store a proc that can provide a Redis instance or mock
  def add_external_redis_builder(builder) #callable
    @redis_builder = builder
    self
  end

  # use to apply an instance directly and bypass the builder
  def use_redis(instance)
    @redis = instance
    self
  end

  def new_redis_instance
    @redis_builder.call
  end

  private

  # private
  def redis_params
    if @path.nil?
      connection_params = {
          :host => @host,
          :port => @port
      }
    else
      @logger.warn("Parameter 'path' is set, ignoring parameters: 'host' and 'port'")
      connection_params = {
          :path => @path
      }
    end

    base_params = {
        :timeout => @timeout,
        :db => @db,
        :password => @password.nil? ? nil : @password.value,
        :ssl => @ssl
    }

    connection_params.merge(base_params)
  end

  # private
  def internal_redis_builder
    ::Redis.new(redis_params)
  end

  # private
  def reset_redis
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end

  # private
  def connect(batch=false)
    redis = new_redis_instance
    # register any renamed Redis commands
    if @command_map.any?
      client_command_map = redis.client.command_map
      @command_map.each do |name, renamed|
        client_command_map[name.to_sym] = renamed.to_sym
      end
    end
    load_batch_script(redis) if batch
    redis
  end

  # private
  def load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    redis_script = <<EOF
  local batchsize = tonumber(ARGV[1])
  local result = redis.call(\'#{@command_map.fetch('lrange', 'lrange')}\', KEYS[1], 0, batchsize)
  redis.call(\'#{@command_map.fetch('ltrim', 'ltrim')}\', KEYS[1], batchsize + 1, -1)
  return result
EOF
    @redis_script_sha = redis.script(:load, redis_script)
  end

  # private
  def redis_runner(batched=false)
    begin
      @redis ||= connect(batched)
      yield
    rescue ::Redis::BaseError => e
      @logger.warn("Redis connection problem", :exception => e)
      # Reset the redis variable to trigger reconnect
      @redis = nil
      Stud.stoppable_sleep(1) { stop? }
      retry unless stop?
    end
  end
end end end # RedisConnection PluginMixins LogStash
