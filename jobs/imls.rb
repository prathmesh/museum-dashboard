#!/usr/bin/env ruby
require 'soda'


# Configure the dataset ID and initialize SODA client
dataset_resource_id = "5rw9-2vgh"
soda_client = SODA::Client.new({
  domain: "data.imls.gov",
  app_token: "CSCIzU9TS2FyBGW5XezpQ7aS7"
})

DATA_DICTIONARY = {
  "museum_type" => {
    "ART" => "Art Museums",
    "BOT" => "Arboretums, Botanical Gardens, & Nature Centers",
    "CMU" => "Children's Museums",
    "GMU" => "Uncategorized or General Museums",
    "HSC" => "Historical Societies, Historic Preservation",
    "HST" => "History Museums",
    "NAT" => "Natural History & Natural Science Museums",
    "SCI" => "Science & Technology Museums & Planetariums",
    "ZAW" => "Zoos, Aquariums, & Wildlife Conservation",
  },
  "nces_locale_code" => {
    "1" => "City",
    "2" => "Suburb",
    "3" => "Town",
    "4" => "Rural"
  },
  "aam_museum_region" => {
    "1" => "New England",
    "2" => "Mid-Atlantic",
    "3" => "Southeastern",
    "4" => "Midwest",
    "5" => "Mount Plains",
    "6" => "Western"
  },
  "micropolitan_area_flag" => {
    "0" => "Not in a micropolitan statstical area (µSA)",
    "1" => "In a micropolitan statistical area (µSA)"
  },
  "irs_990_flag" => {
    "0" => "IRS form 990 data source not used",
    "1" => "IRS form 990 data source used"
  },
  "imls_admin_data_source_flag" => {
    "0" => "IMLS administrative data source not used",
    "1" => "IMLS administrative data source used"
  },
  "third_party_source_flag" => {
    "0" => "Third party (Factual) source not used",
    "1" => "Third party (Factual) source used"
  },
  "private_grant_foundation_data_source_flag" => {
    "0" => "Private grant foundation data source not used",
    "1" => "Private grant foundation data source used"
  }
}

SCHEDULER.every '60m', first_in: 0 do |job|


    # #### COUNT BY MUSUEM TYPE ####
  # Construct SODA query
  count_by_type_response = soda_client.get(dataset_resource_id, {
    "$group" => "museum_type",
    "$select" => "museum_type, COUNT(*) AS n",
    "$where" => "state = 'DC'"
  })
  # Formulate list
  count_by_type = {}
  count_by_type_response.each do |item|
    type_humanized = DATA_DICTIONARY["museum_type"][item.museum_type]
    count_by_type[type_humanized] = {:label => type_humanized, :value => item.n}
  end
  # Send event to dashboard
  send_event('count_by_type', { items: count_by_type.values.sort_by{|x| x[:value].to_i}.reverse })




  # #### TOTAL Museums ####
  total_museums_response = soda_client.get(dataset_resource_id, {
    "$select" => "count(*)",
    "$where" => "state = 'DC'"
  })
  total_museums = total_museums_response.first["count"].to_i
  puts total_museums.inspect
  send_event('total_museums', { current:  total_museums})

  # #### TOTAL INCOME of all non profits ####
   total_income = soda_client.get(dataset_resource_id, {
  	 "$select" => "SUM(total_revenue) AS n",
     "$where" => " irs_990_flag = '1'"
   })
   total_income = total_income.first["n"]
   send_event('total_income', { current:  total_income})
   puts total_income.inspect

#### TOTAL NONPROFIT ####
  total_nonprofit_response = soda_client.get(dataset_resource_id, {
	"$where" => "state = 'DC' and ein is not null",
	"$select" => "count(*)"
    
  })
  total_nonprofit = total_nonprofit_response.first["count"]
  send_event('total_nonprofit', { current:  total_nonprofit})
  puts total_nonprofit.inspect


 # #### PERCENT NONPROFIT ####
  percent_nonprofit = ((total_nonprofit.to_f/total_museums.to_f)*100).to_i
  send_event('percent_nonprofit', { value:  percent_nonprofit})
  
  # #### COUNT BY ISSUE TYPE ####
   count_by_museum_type_response = soda_client.get(dataset_resource_id, {
     "$group" => "museum_type",
     "$select" => "museum_type, COUNT(museum_type) AS n",
     "$where" => "state = 'DC'"
   })
   count_by_museum_type = {}
   count_by_museum_type_response.each do |item|
     count_by_museum_type[item.museum_type] = {:label => item.museum_type, :value => item.n}
   end
  # # Send event to dashboard
   send_event('count_by_museum_type', { items: count_by_museum_type.values.sort_by{|x| x[:value].to_i}.reverse })

end
