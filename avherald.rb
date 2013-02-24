require "logstash/inputs/base"
require "net/http"
require "nokogiri"
require "date"

class LogStash::Inputs::Avherald < LogStash::Inputs::Base

  config_name "avherald"
  plugin_status "experimental"

  AVHERALD_URL = 'http://avherald.com/'

  # The Airline Company you want to retrieve the issue for
  config :airline, :validate => :array

  # The plane model you want to retrieve the issue for
  config :model, :validate => :array

  # The city concerned you want to retrieve the issue for
  config :city, :validate => :array

  # The keywords you want to retrieve the reason of the issue for
  config :keywords, :validate => :array

  # How ofter you want to webpage to be checked for new entries (in second)
  config :stat_interval, :validate =>  :number, :default => 300

  # Where to write the last log you did receive (in case of the logstash agent get stopped)
  config :sincedb_path, :validate => :string

  # From where should the log be retrieved, beginning, end, last, #id
  config :start_position, :validate => :string,  :default => 'end'

  public
  def initialize(params)
    super

    @format = 'plain'
    @model ||= []
    @airline ||= []
    @city ||= []
    @keywords ||= []
    @current_id = @start_position if !['beginning', 'end'].include? @start_position
  end

  public
  def register
    @logger.info("Registering AvHerald input")

    if @sincedb_path.nil?
      if ENV["HOME"].nil?
        @logger.error("No HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME in your environment, or set sincedb_path in " \
                      "in your logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise
      end
      @sincedb_path = "#{ENV['HOME']}/.sincedb_avherald"
    end
    @current_id = File.open(@sincedb_path) {|f| f.readline } if @start_position.eql? 'last'
  end

  public
  def run(queue)
    loop do

      track do |incident|

        @logger.info("Currently dealing with #{incident['full_text']}")

        e = to_event(incident['full_text'], "http://avherald.com/h?article=#{incident['avid']}&opt=0")

        e.fields.merge!(
          'model' => (incident['model'] rescue nil),
          'airline' => (incident['airline'] rescue nil),
          'city' => (incident['city'] rescue nil),
          'date' => (incident['date'] rescue nil),
          'reason' => (incident['reason'] rescue nil),
          'type' => (incident['type'] rescue nil),
          'avid' => (incident['avid'] rescue nil)
        )

        queue << e
      end

      sleep 5
    end
  end

  private
  def get_content(url, content)

    # Building the actual URL from where the data needs to be retrieved
    actual_url =  url.query.instance_of?(NilClass) ? url.path : "#{url.path}?#{url.query}"
      req = Net::HTTP::Get.new(actual_url)
    req['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    req['Connection'] = 'keep-alive'
    req['Pragma'] = 'no-cache'
    req['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:18.0) Gecko/20100101 Firefox/18.0'
    res = Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)
    }

    html_doc = Nokogiri::HTML(res.body)
    spans =  html_doc.xpath("//span[@class='headline_avherald']/../../..")

    # Merging NodeSet
    content = (content.nil?) ? spans : content + spans

    # Checking if we reached the end of the incidents
    # Retrieving link to the next page
    next_img = html_doc.xpath("//img[@src='/images/next.jpg']")
    next_link = html_doc.xpath("//img[@src='/images/next.jpg']/../@href")
    new_url = URI.parse("#{AVHERALD_URL}#{next_link.to_s[1..-1]}")

    return content if !html_doc.xpath("//span[@class='headline_avherald']/../@href[starts-with(.,'/h?article=#{@current_id}&opt=0')]").empty? or next_img.nil?
    return get_content(new_url, content) unless next_img.nil?
  end # def get_content(url, content)

  private
  def track

    spans = get_content(URI.parse(AVHERALD_URL), nil) unless @start_position.eql? 'end'

    incident_h = Hash.new
    spans.each do |incident|
      log = Nokogiri::HTML(incident.to_s)

      break if log.xpath('//a/@href').to_s[/h?article=(.*)&opt=0/, 1].eql? @current_id

      incident_h['type'] = log.xpath('//img/@alt').to_s
      incident_h['avid'] = log.xpath('//a/@href').to_s[/h?article=(.*)&opt=0/, 1]
      content = log.xpath('//span[@class="headline_avherald"]/text()')
      incident_h['full_text'] = content.to_s
      incident_h['airline'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 1]
      incident_h['model'] = content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 2]
      incident_h['city'] = content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 4]
      incident_h['date'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 5]
      incident_h['reason'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 6]

      if (@model.include? incident_h['model'] or @model.empty?) \
        and (@airline.include? incident_h['airline'] or @airline.empty?) \
        and (@city.include? incident_h['city'] or @city.empty?)

        yield incident_h if @keywords.empty?

        incident_h['reason'].split.each do |keyword|
          yield incident_h if @keywords.include? keyword
        end unless @keywords.empty?

      end
    end unless @start_position.eql? 'end'
    @current_id = Nokogiri::HTML(spans[0].to_s).xpath('//a/@href').to_s[/h?article=(.*)&opt=0/, 1]
  end

  public
  def teardown
    File.open(@sincedb_path, 'w') { |f| f.write(@current_id) }
  end

end
