require "logstash/inputs/base"
require "net/http"
require "nokogiri"

class LogStash::Inputs::Avherald < LogStash::Inputs::Base

  config_name "avherald"
  plugin_status "experimental"

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

  # From where should the log be retrieved
  config :start_position, :default => 'end'

  public
  def initialize(params)
    super

    @format = 'plain'
  end

  public
  def register
    @logger.info("Registering AvHerald input")
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

      sleep 60
    end
  end

  private
  def track
    url = URI.parse('http://avherald.com/')

    req = Net::HTTP::Get.new(url.path)
    req['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    req['Connection'] = 'keep-alive'
    req['Pragma'] = 'no-cache'
    req['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:18.0) Gecko/20100101 Firefox/18.0'
    res = Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)
    }

    html_doc = Nokogiri::HTML(res.body)
    spans =  html_doc.xpath("//span[@class='headline_avherald']/../../..")
    incident_h = Hash.new
    spans.each do |incident|
      log = Nokogiri::HTML(incident.to_s)

      incident_h['type'] = log.xpath('//img/@alt').to_s
      incident_h['avid'] = log.xpath('//a/@href').to_s[/h?article=(.*)&opt=0/, 1]
      content = log.xpath('//span[@class="headline_avherald"]/text()')
      incident_h['full_text'] = content.to_s
      incident_h['airline'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 1]
      incident_h['model'] = content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 2]
      incident_h['city'] = content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 4]
      incident_h['date'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 5]
      incident_h['reason'] =  content.to_s[/(.*) ([A-Z0-9]*) (at|over|enroute|near) (.*) on (.*), (.*)/, 6]

      yield incident_h

    end
  end

end