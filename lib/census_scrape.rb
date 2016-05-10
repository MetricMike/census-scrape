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
    @csv = CSV.open("acs5.csv", "w+", headers: true, force_quotes: true)
    @csv << %w(state cd sldu sldl county tract bg field_id label value)
    setup_acs_vars
  end

  def scrape
    state_ids = write_state_fields #est 10 minutes
    write_cd_fields(state_ids) #est +20 minutes
    # cds = [] # 52 hashes of X sldl
    # states.each do |state|
    #   next if state.nil?
    #   tot_cds = client.where({ fields: total_fields, level: 'CD', within: "STATE:#{state['state']}"})
    #   age_cds = client.where({ fields: age_fields, level: 'CD', within: "STATE:#{state['state']}"})
    #   cds << tot_cds.map{ |h| age_cds.map { |i| h.merge(i) if i['cd']==h['cd'] } }.flatten.compact
    # end

    # sldus = [] # 52 hashes of X sldu
    # states.each do |state|
    #   next if state.nil?
    #   tot_sldus = client.where({ fields: total_fields, level: 'SLDU', within: "STATE:#{state['state']}"})
    #   age_sldus = client.where({ fields: age_fields, level: 'SLDU', within: "STATE:#{state['state']}"})
    #   sldus << tot_sldus.map{ |h| age_sldus.map { |i| h.merge(i) if i['state legislative district (upper chamber)']==h['state legislative district (upper chamber)'] } }.flatten.compact
    # end

    # sldls = [] # 52 hashes of X sldl
    # states.each do |state|
    #   tot_sldls = client.where({ fields: total_fields, level: 'SLDL', within: "STATE:#{state['state']}"})
    #   age_sldls = client.where({ fields: age_fields, level: 'SLDL', within: "STATE:#{state['state']}"})
    #   sldls << tot_sldls.map{ |h| age_sldls.map { |i| h.merge(i) if i['state legislative district (lower chamber)']==h['state legislative district (lower chamber)'] } }.flatten.compact
    # end

    # counties = [] # 52 hashes of X counties
    # states.each do |state|
    #   next if state.nil?
    #   tot_counties = client.where({ fields: total_fields, level: 'COUNTY', within: "STATE:#{state['state']}"})
    #   age_counties = client.where({ fields: age_fields, level: 'COUNTY', within: "STATE:#{state['state']}"})
    #   counties << tot_counties.map{ |h| age_counties.map { |i| h.merge(i) if i['county']==h['county'] } }.flatten.compact
    # end

    # tracts = []
    # counties.each do |county_by_state|
    #   county_by_state.each do |county|
    #     tot_tracts = client.where({ fields: total_fields, level: 'TRACT', within: "STATE:#{county['state']}+COUNTY:#{county['county']}"})
    #     age_tracts = client.where({ fields: age_fields, level: 'TRACT', within: "STATE:#{county['state']}+COUNTY:#{county['county']}"})
    #     tracts << tot_tracts.map{ |h| age_tracts.map { |i| h.merge(i) if i['tract']==h['tract'] } }.flatten.compact
    #   end
    # end

    # block_groups = []
    # tracts.each do |tracts_by_county|
    #   tracts_by_county.each do |tract|
    #     tot_block_groups = client.where({ fields: total_fields, level: 'BG', within: "STATE:#{tract['state']}+COUNTY:#{tract['county']}+TRACT:#{tract['tract']}"})
    #     age_block_groups = client.where({ fields: age_fields, level: 'BG', within: "STATE:#{tract['state']}+COUNTY:#{tract['county']}+TRACT:#{tract['tract']}"})
    #     block_groups << tot_block_groups.map{ |h| age_block_groups.map { |i| h.merge(i) if i['block group']==h['block group'] } }.flatten.compact
    #   end
    # end

      # cds.map do |cds_by_state|
      #   next if cds_by_state.nil?
      #   cds_by_state.map do |cd|
      #     all_fields.map do |f|
      #       csv << [ cd['state'], cd['cd'], "", "", "", "", "", f, acs_vars[f]['concept']+acs_vars[f]['label'], cd[f] ]
      #     end
      #   end
      # end

      # sldus.map do |sldus_by_state|
      #   next if sldus_by_state.nil?
      #   sldus_by_state.map do |sldu|
      #     all_fields.map do |f|
      #       csv << [ sldu['state'], "", sldu['sldu'], "", "", "", "", f, acs_vars[f]['concept']+acs_vars[f]['label'], sldu[f] ]
      #     end
      #   end
      # end

      # sldls.map do |sldls_by_state|
      #   next if sldls_by_state.nil?
      #   sldls_by_state.map do |sldl|
      #     all_fields.map do |f|
      #       csv << [ sldl['state'], "", "", sldl['sldl'], "", "", "", f, acs_vars[f]['concept']+acs_vars[f]['label'], sldl[f] ]
      #     end
      #   end
      # end

      # counties.map do |counties_by_state|
      #   next if counties_by_state.nil?
      #   counties_by_state.map do |county|
      #     all_fields.map do |f|
      #       csv << [ county['state'], "", "", "", county['county'], "", "", f, acs_vars[f]['concept']+acs_vars[f]['label'], county[f] ]
      #     end
      #   end
      # end

      # tracts.map do |tracts_by_state|
      #   next if tracts_by_state.nil?
      #   tracts_by_state.map do |tracts_by_county|
      #     tracts_by_county.map do |tract|
      #       all_fields.map do |f|
      #         csv << [ tract['state'], "", "", "", tract['county'], tract['tract'], "", f, acs_vars[f]['concept']+acs_vars[f]['label'], tract[f] ]
      #       end
      #     end
      #   end
      # end

      # block_groups.map do |block_groups_by_state|
      #   next if block_groups_by_state.nil?
      #   block_groups_by_state.map do |block_groups_by_county|
      #     block_groups_by_county.map do |block_groups_by_tract|
      #       block_groups_by_tract.map do |block_group|
      #         all_fields.map do |f|
      #           csv << [ block_group['state'], "", "", "", block_group['county'], block_group['tract'], block_group["block group"], f, acs_vars[f]['concept']+acs_vars[f]['label'], block_group[f] ]
      #         end
      #       end
      #     end
      #   end
      # end

  end

  def write_state_fields
    ids = []
    @@acs_vars.keys.each_slice(49) do |field_group|
      @@client.where({ fields: field_group.join(','), level: 'STATE' }).map do |state_hash|
        ids << state_hash['state']
        field_group.map do |f|
          @csv << [ state_hash['state'], "", "", "", "", "", "", f, @@acs_vars[f]['concept']+@@acs_vars[f]['label'], state_hash[f] ]
        end
      end
    end
    ids.uniq
  end

  def write_cd_fields(state_ids)
    @@acs_vars.keys.each_slice(25) do |field_group|
      @@client.where({ fields: field_group.join(','), level: 'CD', within: "STATE:#{state_ids.join(',')}" }).map do |cd_hash|
        field_group.map do |f|
          @csv << [ cd_hash['state'], cd_hash['congressional district'], "", "", "", "", "", f, @@acs_vars[f]['concept']+@@acs_vars[f]['label'], cd_hash[f] ]
        end
      end
    end
  end
end
end
