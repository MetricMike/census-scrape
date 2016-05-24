require "census_scrape/version"
require 'census_api'
require 'csv'
require 'json'
require 'pry'

module CensusScrape
class Scraper
  @@acs_vars = []
  @@client = CensusApi::Client.new(ENV['CENSUS_API_KEY'], dataset: "ACS5", vintage: 2014)
  USELESS_KEYS = %w(for in)

  def setup_acs_vars
    acs = JSON.load(File.new("lib/census_scrape/acs_variables.json"))["variables"]; 0;
    USELESS_KEYS.map { |bad_key| acs.delete(bad_key) }; 0;
    @@acs_vars = acs.reject! { |k,v| k[-1] == 'M' || v['concept'] == "Selectable Geographies" }; 0;
  end

  def initialize
    @csv = CSV.open("acs5-#{Time.now.to_i}.csv", "w+", headers: true, force_quotes: true)
    @csv << %w(loc_id state cd sldu sldl county tract bg field_id value)
    setup_acs_vars
  end

  def reset_csv(suffix=Time.now.to_i)
    @csv.close
    @csv = CSV.open("acs5-#{suffix}.csv", "w+", headers: true, force_quotes: true)
    @csv << %w(loc_id state cd sldu sldl county tract bg field_id value)
  end

  def state_ids
    @state_ids ||= @@client.where({fields: @@acs_vars.keys.first, level: 'STATE'}).map { |h| h['state'] }
  end

  def county_ids_for_state(state_id)
    @@client.where({fields: @@acs_vars.keys.first, level: 'COUNTY', within: "STATE:#{state_id}"}).map { |h| h['county'] }
  end

  def tract_ids_for_county_and_state(state_id, county_id)
    @@clients.where({fields: @@acs_vars.keys.first, level: 'TRACT', within: "STATE:#{state_id}+COUNTY:#{county_id}"}).map { |h| h['tract'] }
  end

  def scrape
    # state_ids = write_state_fields #est 10 minutes
    # reset_csv
    # write_acs_vars
    # reset_csv

    # state_ids.each_slice(10) do |state_slice|
    #   next if state_slice.first.to_i < 45
    #   reset_csv("sldu-from-#{state_slice.first}-to-#{state_slice.last}")
    #   write_sldu_fields(state_slice) #est +6 minutes
    # end

    # state_ids.each_slice(10) do |state_slice|
    #   reset_csv("sldl-from-#{state_slice.first}-to-#{state_slice.last}")
    #   write_sldl_fields(state_slice) #est +6 minutes
    # end

    # state_ids.each_slice(10) do |state_slice|
    #   next if state_slice.first.to_i < 45
    #   reset_csv("counties-from-#{state_slice.first}-to-#{state_slice.last}")
    #   write_county_fields(state_slice)
    # end

    state_ids.each do |state_id|
      next if state_id.to_i < 5
      reset_csv("tracts-for-state-#{state_id}")
      write_tract_fields(county_ids_for_state(state_id), state_id)
    end
  end

  def write_acs_vars
    @csv.close
    @csv = CSV.open("acs5-vars.csv", "w+", headers: true, force_quotes: true)
    @csv << %w(field_id label concept predicateType)

    @@acs_vars.keys.map do |key|
      @csv << [ key, @@acs_vars[key]['label'], @@acs_vars[key]['concept'], @@acs_vars[key]['predicateType'] ]
    end
  end

  def write_state_fields
    ids = []
    @@acs_vars.keys.each_slice(45) do |field_group|
      slice_batch = []
      @@client.where({ fields: field_group.join(','), level: 'STATE' }).map do |state_hash|
        ids << state_hash['state']
        field_group.map do |f|
          slice_batch << [ build_location(state_hash), state_hash['state'], "", "", "", "", "", "", f, state_hash[f] ]
        end
      end
      slice_batch.map { |row| @csv << row }
    end
    ids.uniq
  end

  def write_cd_fields(state_ids)
    ids = []
    @@acs_vars.keys.each_slice(45) do |field_group|
      slice_batch = []
      @@client.where({ fields: field_group.join(','), level: 'CD', within: "STATE:#{state_ids.join(',')}" }).map do |cd_hash|
        ids << cd_hash['congressional district']
        field_group.map do |f|
          next unless cd_hash[f]
          slice_batch << [ build_location(cd_hash), cd_hash['state'], cd_hash['congressional district'], "", "", "", "", "", f, cd_hash[f] ]
        end
      end
      slice_batch.map { |row| @csv << row }
    end
    ids.uniq
  end

  def write_sldu_fields(state_ids)
    state_ids.map do |state_id|
      @@acs_vars.keys.each_slice(45) do |field_group|
        slice_batch = []
        @@client.where({ fields: field_group.join(','), level: 'SLDU', within: "STATE:#{state_id}" }).map do |sldu_hash|
          next unless sldu_hash.is_a? Hash
          field_group.map do |f|
            next unless sldu_hash[f]
            slice_batch << [ build_location(sldu_hash), sldu_hash['state'], "", sldu_hash['state legislative district (upper chamber)'], "", "", "", "", f, sldu_hash[f] ]
          end
        end
        slice_batch.map { |row| @csv << row }
      end
    end
  end

  def write_sldl_fields(state_ids)
    state_ids.map do |state_id|
      @@acs_vars.keys.each_slice(45) do |field_group|
        slice_batch = []
        @@client.where({ fields: field_group.join(','), level: 'SLDL', within: "STATE:#{state_id}" }).map do |sldl_hash|
          next unless sldl_hash.is_a? Hash
          field_group.map do |f|
            next unless sldl_hash[f]
            slice_batch << [ build_location(sldl_hash), sldl_hash['state'], "", "", sldl_hash['state legislative district (lower chamber)'], "", "", "", f, sldl_hash[f] ]
          end
        end
        slice_batch.map { |row| @csv << row }
      end
    end
  end

  def write_county_fields(state_ids)
    state_ids.map do |state_id|
      @@acs_vars.keys.each_slice(45) do |field_group|
        slice_batch = []
        @@client.where({ fields: field_group.join(','), level: 'COUNTY', within: "STATE:#{state_id}" }).map do |county_hash|
          next unless county_hash.is_a? Hash
          field_group.map do |f|
            next unless county_hash[f]
            slice_batch << [ build_location(county_hash), county_hash['state'], "", "", "", county_hash['county'], "", "", f, county_hash[f] ]
          end
        end
        slice_batch.map { |row| @csv << row }
      end
    end
  end

  def write_tract_fields(county_ids, state_id)
    @@acs_vars.keys.each_slice(45) do |field_group|
      slice_batch = []
      @@client.where({ fields: field_group.join(','), level: 'TRACT', within: "STATE:#{state_id}+COUNTY:#{county_ids.join(',')}" }).map do |tract_hash|
        next unless tract_hash.is_a? Hash
        field_group.map do |f|
          next unless tract_hash[f]
          slice_batch << [ build_location(tract_hash), tract_hash['state'], "", "", "", tract_hash['county'], tract_hash['tract'], "", f, tract_hash[f] ]
        end
      end
      slice_batch.map { |row| @csv << row }
    end
  end

  def build_location(hash)
    ['state', 'congressional district',
     'state legislative district (upper chamber)',
     'state legislative district (lower chamber)',
     'county', 'tract', 'block group'].map do |loc|
      hash[loc] || 'x'
    end.join('')
  end
end
end
