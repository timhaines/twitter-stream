require 'eventmachine'
require 'em/buftok'
require 'uri'
require 'roauth'

module Twitter
  class JSONStream < EventMachine::Connection
    MAX_LINE_LENGTH = 1024*1024*1024

    # network failure reconnections
    NF_RECONNECT_START = 0.25
    NF_RECONNECT_ADD   = 1.00
    NF_RECONNECT_MAX   = 10

    # app failure reconnections
    AF_RECONNECT_START = 1
    AF_RECONNECT_MUL   = 1.3

    RECONNECT_MAX   = 320
    RETRIES_MAX     = 10

    DEFAULT_OPTIONS = {
      :method       => 'GET',
      :path         => '/',
      :content_type => "application/x-www-form-urlencoded",
      :content      => '',
      :path         => '/1/statuses/filter.json',
      :host         => 'stream.twitter.com',
      :port         => 443,
      :ssl          => true,
      :user_agent   => 'Favstar',
      :timeout      => 0,
      :proxy        => ENV['HTTP_PROXY'],
      :auth         => nil,
      :oauth        => {},
      :params       => {},
      :logger       => nil
    }

    attr_accessor :code
    attr_accessor :headers
    attr_accessor :nf_last_reconnect
    attr_accessor :af_last_reconnect
    attr_accessor :reconnect_retries
    attr_accessor :proxy
    attr_accessor :logger
    
    def self.connect options = {}
      options[:port] = 443 if options[:ssl] && !options.has_key?(:port)
      options = DEFAULT_OPTIONS.merge(options)

      host = options[:host]
      port = options[:port]
      
      

      if options[:proxy]
        proxy_uri = URI.parse(options[:proxy])
        host = proxy_uri.host
        port = proxy_uri.port
      end

      # options[:logger].info "** Connecting with #{host} #{port} #{options.inspect}" if options[:logger]
      connection = EventMachine.connect host, port, self, options
      connection.start_tls if options[:ssl]
      connection.pending_connect_timeout = 5.0
      # connection.comm_inactivity_timeout = 5.0
            
      connection
    end

    def initialize options = {}
      @options = DEFAULT_OPTIONS.merge(options) # merge in case initialize called directly
      @gracefully_closed = false
      @nf_last_reconnect = nil
      @af_last_reconnect = nil
      @reconnect_retries = 0
      @immediate_reconnect = false
      @proxy = URI.parse(options[:proxy]) if options[:proxy]
      @logger = options[:logger]
    end
    
    def log msg
      @logger.info "Sig #{signature}: - #{msg}" if @logger
      puts "No Logger!" unless @logger
    end  
    
    def warn msg
      @logger.warn "Sig #{signature}: #{msg}" if @logger
    end  

    def each_item &block
      @each_item_callback = block
    end

    def on_ping &block
      @ping_callback = block
    end  

    def on_error &block
      @error_callback = block
    end
    
    def on_connect &block
      @connect_callback = block
    end  

    def on_reconnect &block
      @reconnect_callback = block
    end

    def on_max_reconnects &block
      @max_reconnects_callback = block
    end

    def stop
      @gracefully_closed = true
      close_connection
    end

    def immediate_reconnect
      @immediate_reconnect = true
      @gracefully_closed = false
      close_connection
    end

    def unbind
      # log "Unbinding!  Retries is at #{reconnect_retries} and inactivity timeout is at #{comm_inactivity_timeout}"      
      receive_line(@buffer.flush) unless @buffer.empty?
      @state   = :init
      EM.add_timer(0) do
        schedule_reconnect unless @gracefully_closed
      end  
    end

    def receive_data data
      begin
        @buffer.extract(data).each do |line|
          receive_line(line)
        end
      rescue Exception => e
        receive_error("#{e.class}: " + [e.message, e.backtrace].flatten.join("\n\t"))
        close_connection
        return
      end
    end

    def connection_completed
      @connect_callback.call if @connect_callback
      send_request
    end

    def post_init
      log "Post init called - reseting state! This happens before headers are read."
      reset_state 
    end

    def schedule_reconnect
      timeout = reconnect_timeout
      log "Scheduling reconnect in #{timeout} seconds - received code #{@code} previously"      
      reset_state
      @reconnect_retries += 1
      if timeout <= RECONNECT_MAX && @reconnect_retries <= RETRIES_MAX
        reconnect_after(timeout)
      else
        warn "Max reconnects hit!  Calling callback."
        @max_reconnects_callback.call(timeout, @reconnect_retries) if @max_reconnects_callback
      end
    end

  protected

    def reconnect_after timeout
      @reconnect_callback.call(timeout, @reconnect_retries) if @reconnect_callback

      if timeout == 0
        reconnect @options[:host], @options[:port]
      else
        log "Will reconnect after #{timeout}"
        EventMachine.add_timer(timeout) do
          log "Reconnecting!"
          begin
            reconnect @options[:host], @options[:port]
          rescue => e
            receive_error("Error on Reconnect: #{e.class}: " + [e.message, e.backtrace].flatten.join("\n\t"))
          end    
        end
        
        EventMachine.add_timer(timeout) do
          log "That's #{timeout}!"
        end
        
      end
    end

    def reconnect_timeout
      if @immediate_reconnect
        @immediate_reconnect = false
        return 0
      end

      if (@code == 0) # network failure
        if @nf_last_reconnect
          @nf_last_reconnect += NF_RECONNECT_ADD
        else
          @nf_last_reconnect = NF_RECONNECT_START
        end
        [@nf_last_reconnect,NF_RECONNECT_MAX].min
      else
        if @af_last_reconnect
          @af_last_reconnect *= AF_RECONNECT_MUL
        else
          @af_last_reconnect = AF_RECONNECT_START
        end
        @af_last_reconnect
      end
    end

    def reset_state code=0
      log "reseting state"
      set_comm_inactivity_timeout @options[:timeout] if @options[:timeout] > 0
      @code    = code
      @headers = []
      @state   = :init
      @buffer  = BufferedTokenizer.new("\r", MAX_LINE_LENGTH)
    end

    def send_request
      data = []
      request_uri = @options[:path]

      if @proxy
        # proxies need the request to be for the full url
        request_uri = "http#{'s' if @options[:ssl]}://#{@options[:host]}:#{@options[:port]}#{request_uri}"
      end

      content = @options[:content]

      if !@options[:params].empty?
        if @options[:method].to_s.upcase == 'GET'
          request_uri << "?#{query}"
        else
          content = query
        end
      end

      data << "#{@options[:method]} #{request_uri} HTTP/1.1"
      data << "Host: #{@options[:host]}"
      data << 'Accept: */*'
      data << "User-Agent: #{@options[:user_agent]}" if @options[:user_agent]

      if @options[:auth]
        data << "Authorization: Basic #{[@options[:auth]].pack('m').delete("\r\n")}"
      elsif @options[:oauth]
        data << "Authorization: #{oauth_header}"
      end

      if @proxy && @proxy.user
        data << "Proxy-Authorization: Basic " + ["#{@proxy.user}:#{@proxy.password}"].pack('m').delete("\r\n")
      end
      if @options[:method] == 'POST'
        data << "Content-type: #{@options[:content_type]}"
        data << "Content-length: #{content.length}"
      end
      data << "\r\n"

      send_data data.join("\r\n") << content
    end

    def receive_line ln
      case @state
      when :init
        parse_response_line ln
      when :headers
        parse_header_line ln
      when :stream
        parse_stream_line ln
      end
    end

    def receive_error e
      @error_callback.call(e) if @error_callback
    end

    def parse_stream_line ln
      if @code == 200
        ln.strip!
        @ping_callback.call if @ping_callback && ln.empty?
        unless ln.empty?
          if ln[0,1] == '{'
            @each_item_callback.call(ln) if @each_item_callback
          end
        end
      else  
        warn(ln.strip) if ln && ln.strip != ""
        buffer_contents = @buffer.flush.strip
        warn(buffer_contents) if buffer_contents && buffer_contents != ""
      end                  
    end

    def parse_header_line ln
      ln.strip!
      if ln.empty?
        reset_timeouts if @code == 200
        unless @code == 200
          warn(ln) if ln
          buffer_contents = @buffer.flush.strip
          warn(buffer_contents) if buffer_contents && buffer_contents != ""
        end          
        @state = :stream
      else
        headers << ln
      end
    end

    def parse_response_line ln
      if ln =~ /\AHTTP\/1\.[01] ([\d]{3})/
        @code = $1.to_i
        @state = :headers
        unless @code == 200
          warn(ln) 
          warn(@buffer.flush.strip)
        end  
        receive_error("invalid status code: #{@code}. #{ln}") unless @code == 200
      else
        receive_error('invalid response')
        close_connection
      end
    end

    def reset_timeouts
      # log "reseting timeouts!"
      set_comm_inactivity_timeout @options[:timeout] if @options[:timeout] > 0      
      # log "comm_inactivity_timeout is #{comm_inactivity_timeout} because @options[:timeout] is #{@options[:timeout]}"      
      @nf_last_reconnect = @af_last_reconnect = nil
      @reconnect_retries = 0
    end

    # :params => {:follow => [1, 2, 3]}
    # :oauth => {
    #   :consumer_key    => [key],
    #   :consumer_secret => [token],
    #   :access_key      => [access key],
    #   :access_secret   => [access secret]
    # }
    def oauth_header
      uri = "http#{"s" if @options[:ssl]}://#{@options[:host]}#{@options[:path]}"
      #log "Params is #{params}"
      ::ROAuth.header(@options[:oauth], uri, params, @options[:method])
    end

    # Normalized query hash of escaped string keys and escaped string values
    # nil values are skipped
    def params
      params = {}
      @options[:params].each{|key, value| params[key.to_s] = escape((value.class == Array ? value.join(",") : value))}
      params
    end

    def query
      params.map{|pair| pair.join("=")}.sort.join("&")
    end

    def escape str
      URI.escape(str.to_s, /[^a-zA-Z0-9\-\.\_\~]/)
    end
  end
end
