require 'webrobots/version'
require 'webrobots/robotstxt'
require 'uri'
require 'net/https'
require 'thread'
require 'rufus-lru'
if defined?(Nokogiri)
  require 'webrobots/nokogiri'
else
  autoload :Nokogiri, 'webrobots/nokogiri'
end

class WebRobots
  # Creates a WebRobots object for a robot named +user_agent+, with
  # optional +options+.
  #
  # * :http_get => a custom method, proc, or anything that responds to
  #   .call(uri), to be used for fetching robots.txt.  It must return
  #   the response body if successful, return an empty string if the
  #   resource is not found, and return nil or raise any error on
  #   failure.  Redirects should be handled within this proc.
  #
  # * :crawl_delay => determines how to react to Crawl-delay
  #   directives.  If +:sleep+ is given, WebRobots sleeps as demanded
  #   when allowed?(url)/disallowed?(url) is called.  This is the
  #   default behavior.  If +:ignore+ is given, WebRobots does
  #   nothing.  If a custom method, proc, or anything that responds to
  #   .call(delay, last_checked_at), it is called.
  #
  # * :max_cache_size => determines the maximum size of the robots cache.
  #   If +nil+ is given, the cache is unbounded. If an integer
  #   is given, then a LRU cache is created which holds cache entries.
  #   This prevents WebRobots from consuming unbounded memory in long-running
  #   processes.
  def initialize(user_agent, options = nil)
    @user_agent = user_agent

    options ||= {}
    @http_get = options[:http_get] || method(:http_get)
    @max_cache_size = options.fetch(:max_cache_size, nil)
    crawl_delay_handler =
      case value = options[:crawl_delay] || :sleep
      when :ignore
        nil
      when :sleep
        method(:crawl_delay_handler)
      else
        if value.respond_to?(:call)
          value
        else
          raise ArgumentError, "invalid Crawl-delay handler: #{value.inspect}"
        end
      end

    @parser = RobotsTxt::Parser.new(user_agent, crawl_delay_handler)
    @cache_mutex = Mutex.new
    @robotstxt = create_cache()
    @conditions = {}
  end

  # :nodoc:
  def create_cache
    if @max_cache_size
      Rufus::Lru::Hash.new(@max_cache_size)
    else
      Hash.new
    end
  end

  # Flushes robots.txt cache.
  def flush_cache
    @cache_mutex.synchronize {
      @robotstxt.clear
    }
  end

  # Returns the robot name initially given.
  attr_reader :user_agent

  # Tests if the robot is allowed to access a resource at +url+.  If a
  # malformed URI string is given, URI::InvalidURIError is raised.  If
  # a relative URI or a non-HTTP/HTTPS URI is given, ArgumentError is
  # raised.
  def allowed?(url)
    site, request_uri = split_uri(url)
    return true if request_uri == '/robots.txt'
    robots_txt = get_robots_txt(site)
    robots_txt.allow?(request_uri)
  end

  # Equivalent to !allowed?(url).
  def disallowed?(url)
    !allowed?(url)
  end

  # Returns the number of seconds that the configured agent should wait
  # between successive requests to the site identified by +url+ according
  # to the site's robots.txt +Crawl-delay+ directive.
  def crawl_delay(url)
    robots_txt_for(url).crawl_delay()
  end

  # Returns extended option values for a resource at +url+ in a hash
  # with each field name lower-cased.  See allowed?() for a list of
  # errors that may be raised.
  def options(url)
    robots_txt_for(url).options
  end

  # Equivalent to option(url)[token.downcase].
  def option(url, token)
    options(url)[token.downcase]
  end

  # Returns an array of Sitemap URLs.  See allowed?() for a list of
  # errors that may be raised.
  def sitemaps(url)
    robots_txt_for(url).sitemaps
  end

  # Returns an error object if there is an error in fetching or
  # parsing robots.txt of the site +url+.
  def error(url)
    robots_txt_for(url).error
  end

  # Raises the error if there was an error in fetching or parsing
  # robots.txt of the site +url+.
  def error!(url)
    robots_txt_for(url).error!
  end

  # Removes robots.txt cache for the site +url+.
  def reset(url)
    site, = split_uri(url)
    @robotstxt.delete(site.hostname + ":" + site.port.to_s)
  end

  private

  def split_uri(url)
    site =
      if url.is_a?(URI)
        url.dup
      else
        begin
          URI.parse(url)
        rescue => e
          raise ArgumentError, e.message
        end
      end

    site.scheme && site.host or
      raise ArgumentError, "non-absolute URI: #{url}"

    site.is_a?(URI::HTTP) or
      raise ArgumentError, "non-HTTP/HTTPS URI: #{url}"

    request_uri = site.request_uri
    if (host = site.host).match(/[[:upper:]]/)
      site.host = host.downcase
    end
    site.path = '/'
    return site, request_uri
  end

  def robots_txt_for(url)
    site, = split_uri(url)
    get_robots_txt(site)
  end

  def get_robots_txt(site)
    key = site.hostname + ":" + site.port.to_s
    val = @cache_mutex.synchronize {
      @robotstxt[key] if @robotstxt.key?(key)
    }
    if val
      val
    else
      fetch_robots_txt(key, site)
    end
  end

  def fetch_robots_txt(key, site)
    cond = nil
    @cache_mutex.synchronize do
      if @conditions.key? key
        # If someone else is already working on this key, then yield the mutex
        # and wait for them to finish
        @conditions[key].wait(@cache_mutex)

        # After they finish, if they resolved a record, return it
        if @robotstxt[key]
          return @robotstxt[key]
        end

        # Otherwise, continue
      end

      # Create a new ConditionVariable that other threads will wait on
      cond = @conditions[key] = ConditionVariable.new
    end

    begin
      # Perform the call. We don't want this synchronized as it will cause
      # us to only be able to fetch one robots.txt at a time.
      body, result = nil, nil
      begin
        body = @http_get.call(site + 'robots.txt')
      rescue => e
        result = RobotsTxt.unfetchable(site, e, @user_agent)
      end

      # Re-acquire the mutex. All other threads should be waiting on our condition
      # variable above, so we should be able to re-acquire this mutex without significant contention.
      @cache_mutex.synchronize do
        # Parse the result
        if result.nil?
          begin
            raise 'robots.txt unfetchable' if body.nil?
            # The parser is not threadsafe, so it's important that parsing happen in a critical section.
            result = @parser.parse!(body, site)
          rescue => e
            result = RobotsTxt.unfetchable(site, e, @user_agent)
          end
        end

        # Set the result
        @robotstxt[key] = result

        # Delete the condition variable (so that other threads can't wait on it)
        @conditions.delete(key)
        cond.broadcast if cond

        # Return the parse result
        return result
      end
    ensure
      # Ensure we broadcast to all waiting threads, waking them up one at a time
      cond.broadcast if cond
    end
  end

  def http_get(uri)
    referer = nil
    10.times {
      http = Net::HTTP.new(uri.host, uri.port)
      if http.use_ssl = uri.is_a?(URI::HTTPS)
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        http.cert_store = OpenSSL::X509::Store.new.tap { |store|
          store.set_default_paths
        }
      end
      header = { 'User-Agent' => @user_agent }
      header['Referer'] = referer if referer
      # header is destroyed by this in ruby 1.9.2!
      response = http.get(uri.request_uri, header)
      case response
      when Net::HTTPSuccess
        return response.body
      when Net::HTTPRedirection
        referer = uri.to_s
        uri = URI(response['location'])
      when Net::HTTPNotFound
        return ''
      else
        response.value
      end
    }
    raise 'too many HTTP redirects'
  end

  def crawl_delay_handler(delay, last_checked_at)
    if last_checked_at
      delay -= Time.now - last_checked_at
      sleep delay if delay > 0
    end
  end
end
