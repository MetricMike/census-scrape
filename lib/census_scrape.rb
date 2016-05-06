require "census_scrape/version"
require 'census_api'
require 'csv'
require 'json'
require 'pry'

module CensusScrape
  def self.scrape
    total_fields = 'B00001_001E,B00002_001E'
    age_fields = 'B01001_001E'
    (2..9).each { |i| age_fields << ",B01001_00#{i}E" }
    (10..49).each { |i| age_fields << ",B01001_0#{i}E" }
    all_fields = (total_fields + ',' + age_fields).split(',')
    client = CensusApi::Client.new(ENV['CENSUS_API_KEY'], dataset: "ACS5", vintage: 2014)
    acs_vars = JSON.load(File.new("lib/census_scrape/acs_variables.json"))["variables"]

    states = [] # 52 hashes
    tot_states = client.where({ fields: total_fields, level: 'STATE' })
    age_states = client.where({ fields: age_fields, level: 'STATE' })
    states = tot_states.map{ |h| age_states.map { |i| h.merge(i) if i['state']==h['state'] } }.flatten.compact!

    counties = [] # 52 hashes of X counties
    states.each do |state|
      tot_counties = client.where({ fields: total_fields, level: 'COUNTY', within: "STATE:#{state['state']}"})
      age_counties = client.where({ fields: age_fields, level: 'COUNTY', within: "STATE:#{state['state']}"})
      counties << tot_counties.map{ |h| age_counties.map { |i| h.merge(i) if i['county']==h['county'] } }.flatten.compact!
    end

    # cousubs = []
    # counties.each do |county_by_state|
    #   county_by_state.each do |county|
    #     tot_cousubs = client.where({ fields: total_fields, level: 'COUSUB', within: "STATE:#{county['state']}+COUNTY:#{county['county']}"})
    #     age_cousubs = client.where({ fields: age_fields, level: 'COUSUB', within: "STATE:#{county['state']}+COUNTY:#{county['county']}"})
    #     cousubs << tot_cousubs.map{ |h| age_cousubs.map { |i| h.merge(i) if i['cousub']==h['cousub'] } }.flatten.compact!
    #   end
    # end
    # Build a CSV of each State

    CSV.open("acs5.csv", "w+", col_sep: "\t", headers: true, force_quotes: true) do |csv|
      csv << %w(state county cousub field_id label value)

      states.map do |state|
        all_fields.map do |f|
          csv << [ state['name'], "", "", f, acs_vars[f]['concept']+acs_vars[f]['label'], state[f] ]
        end
      end

      counties.map do |counties_by_state|
        next if counties_by_state.nil?
        counties_by_state.map do |county|
          all_fields.map do |f|
            csv << [ county['state'], county['name'], "", f, acs_vars[f]['concept']+acs_vars[f]['label'], county[f] ]
          end
        end
      end

    end
  end
end
