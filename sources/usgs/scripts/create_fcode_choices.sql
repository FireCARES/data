-- Creates an FCODE_CHOICES table which provides
-- fcode types for the usgs.structures table.

CREATE TABLE IF NOT EXISTS usgs.fcode_choices (
    id     integer PRIMARY KEY,
    name    varchar(49)
);

INSERT INTO usgs.fcode_choices VALUES
(81000, 'Transportation Facility'),
(81006, 'Airport Terminal'),
(81008, 'Air Support / Maintenance Facility'),
(81010, 'Air Traffic Control Center / Command Center'),
(81011, 'Boat Ramp / Dock'),
(81012, 'Bridge'),
(81014, 'Bridge:  Light Rail / Subway'),
(81016, 'Bridge:  Railroad'),
(81018, 'Bridge:  Road'),
(81020, 'Border Crossing / Port of Entry'),
(81022, 'Bus Station / Dispatch Facility'),
(81024, 'Ferry Terminal / Dispatch Facility'),
(81025, 'Harbor / Marina'),
(81026, 'Helipad / Heliport / Helispot'),
(81028, 'Launch Facility'),
(81030, 'Launch Pad'),
(81032, 'Light Rail Power Substation'),
(81034, 'Light Rail Station'),
(81036, 'Park and Ride / Commuter Lot'),
(81038, 'Parking Lot Structure / Garage'),
(81040, 'Pier / Wharf / Quay / Mole'),
(81042, 'Port Facility'),
(81044, 'Port Facility: Commercial Port'),
(81046, 'Port Facility: Crane'),
(81048, 'Port Facility: Maintenance and Fuel Facility'),
(81050, 'Port Facility: Modal Transfer Facility'),
(81052, 'Port Facility: Passenger Terminal'),
(81054, 'Port Facility: Warehouse Storage / Container Yard'),
(81056, 'Railroad Facility'),
(81058, 'Railroad Command / Control Facility'),
(81060, 'Railroad Freight Loading Facility'),
(81062, 'Railroad Maintenance / Fuel Facility'),
(81064, 'Railroad Roundhouse / Turntable'),
(81066, 'Railroad Station'),
(81068, 'Railroad Yard'),
(81070, 'Rest Stop / Roadside Park'),
(81072, 'Seaplane Anchorage / Base'),
(81073, 'Snowshed'),
(81074, 'Subway Station'),
(81076, 'Toll Booth / Plaza'),
(81078, 'Truck Stop'),
(81080, 'Tunnel'),
(81082, 'Tunnel:  Light Rail / Subway'),
(81084, 'Tunnel:  Road'),
(81086, 'Tunnel:  Railroad'),
(81088, 'Weigh Station / Inspection Station'),
(70100, 'Agriculture or Livestock Structure'),
(70101, 'Agricultural Experimental Station'),
(70102, 'Food Industry Facility'),
(70103, 'Bakery (Regional)'),
(70104, 'Beverage Bottling Plant'),
(70106, 'Brewery / Distillery / Winery'),
(70108, 'Cannery'),
(70110, 'Corral'),
(70112, 'Dairy'),
(70114, 'Farm / Ranch'),
(70116, 'Feedlot'),
(70118, 'Food Distribution Center'),
(70120, 'Fish Farm / Hatchery'),
(70121, 'Fish Ladder'),
(70122, 'Grain Elevator'),
(70124, 'Grain Mill'),
(70126, 'Greenhouse / Nursery'),
(70128, 'Livestock Complex'),
(70130, 'Meat Processing / Packaging Facility'),
(70132, 'Stockyard / Feedlot'),
(70134, 'Veterinary Hospital / Clinic'),
(71000, 'Industrial Facility'),
(71001, 'Chemical Facility'),
(71002, 'Manufacturing Facility'),
(71004, 'Aircraft Manufacturing Facility'),
(71006, 'Armament Manufacturing Facility'),
(71008, 'Automotive Manufacturing Facility'),
(71010, 'Durable / Non-Durable Goods Facility'),
(71012, 'Explosives Facility'),
(71014, 'Fertilizer Facility'),
(71016, 'Hazardous Materials Facility'),
(71018, 'Hazardous Waste Facility'),
(71020, 'Household Products Facility'),
(71022, 'Landfill'),
(71024, 'Lumber Mill / Saw Mill'),
(71026, 'Maintenance Yard'),
(71028, 'Manufacturing Warehouse'),
(71030, 'Mine'),
(71032, 'Mine Waste Disposal Site'),
(71034, 'Mine Uranium Facility'),
(71036, 'Nuclear Weapons Facility'),
(71038, 'Ore Processing Facility'),
(71040, 'Paper / Pulp Mill'),
(71042, 'Pharmaceutical Plant'),
(71044, 'Semiconductor and Microchip Facility'),
(71046, 'Shipyard'),
(71048, 'Steel Plant'),
(71050, 'Superfund Site'),
(71052, 'Textile Plant'),
(72000, 'Commercial or Retail Facility'),
(72002, 'Corporate Headquarters'),
(72004, 'Gas Station'),
(72006, 'Grocery Store'),
(72008, 'Hotel / Motel'),
(72010, 'Shopping Mall / Complex'),
(72012, 'Warehouse (Retail / Wholesale)'),
(73000, 'Education Facility'),
(73002, 'School'),
(73003, 'School: Elementary'),
(73004, 'School: Middle School'),
(73005, 'School: High School'),
(73006, 'College / University'),
(73007, 'Technical/Trade School'),
(74000, 'Emergency Response or Law Enforcement Facility'),
(74001, 'Ambulance Service'),
(74002, 'American Red Cross Facility'),
(74004, 'Border Patrol'),
(74006, 'Bureau of Alchohol, Tobacco, and Firearms'),
(74008, 'Civil Defense'),
(74010, 'Coast Guard'),
(74012, 'Customs Service'),
(74014, 'Department of Justice'),
(74016, 'Drug Enforcement Agency'),
(74018, 'Federal Bureau of Investigation'),
(74020, 'Federal Emergency Management Agency'),
(74022, 'Fire Equipment Manufacturer'),
(74024, 'Fire Hydrant'),
(74026, 'Fire Station / EMS Station'),
(74028, 'Fire Training Facility / Academy'),
(74030, 'Immigration and Naturalization Service'),
(74032, 'Marshal Service'),
(74034, 'Law Enforcement'),
(74036, 'Prison / Correctional Facility'),
(74037, 'Public Safety Office'),
(74038, 'Search and Rescue Office / Facility'),
(74040, 'Secret Service'),
(74042, 'Transportation Safety Board'),
(74044, 'Office of Emergency Management'),
(74017, 'Emergency Operations Center'),
(75000, 'Energy Facility'),
(75002, 'Energy Distribution Control Facility'),
(75004, 'Natural Gas Facility'),
(75006, 'Nuclear Fuel Plant'),
(75008, 'Nuclear Research Facility'),
(75009, 'Nuclear Waste Processing / Storage Facility'),
(75010, 'Nuclear Weapons Plant'),
(75012, 'Oil / Gas Facility'),
(75014, 'Oil / Gas Well or Field'),
(75016, 'Oil / Gas Extraction or Injection Well'),
(75018, 'Oil / Gas Pumping Station'),
(75020, 'Oil / Gas Refinery'),
(75022, 'Oil / Gas Processing Plant'),
(75024, 'Oil / Gas Storage Facility / Tank Farm'),
(75026, 'POL Storage Tank'),
(75028, 'Strategic Petroleum Reserve'),
(75030, 'Electric Facility'),
(75032, 'Hydroelectric Facility'),
(75034, 'Nuclear Facility'),
(75036, 'Solar Facility'),
(75038, 'Substation'),
(75039, 'Coal Facility'),
(75040, 'Wind Facility'),
(75041, 'Waste / Biomass Facility'),
(75042, 'Tidal Facility'),
(75043, 'Geothermal Facility'),
(76000, 'Banking or Finance Facility'),
(76004, 'Bank'),
(76006, 'Bullion Repository'),
(76008, 'Check Clearing Center'),
(76010, 'Commodities Exchange'),
(76012, 'Federal Reserve Bank / Branch'),
(76014, 'Financial Processing Center'),
(76016, 'Financial Services Company'),
(76018, 'Investment / Brokerage Center'),
(76020, 'Insurance and Finance Center'),
(76022, 'Stock Exchange'),
(76024, 'US Mint / Bureau of Engraving and Printing'),
(78000, 'Mail or Shipping Facility'),
(78002, 'Air Shipping Hub'),
(78004, 'Bulk Mail Center'),
(78006, 'Post Office'),
(78008, 'Private and Express Shipping Facility'),
(79000, 'Building General'),
(79002, 'Mobile Home Park'),
(79004, 'Multi-Family Dwelling'),
(79006, 'Single-Family Dwelling'),
(79008, 'Institutional Residence / Dorm / Barracks'),
(80000, 'Health or Medical Facility'),
(80002, 'Blood Bank'),
(80004, 'Center for Disease Control Office'),
(80006, 'Day Care Facility'),
(80008, 'Diagnostic Laboratory'),
(80009, 'Emergency Shelter'),
(80010, 'Homeless Shelter'),
(80011, 'Hospice'),
(80012, 'Hospital / Medical Center'),
(80014, 'Medical Research Laboratory'),
(80016, 'Medical Stockpile Facility'),
(80018, 'Morgue'),
(80020, 'Mortuary / Crematory'),
(80022, 'Nursing Home / Long Term Care'),
(80024, 'Outpatient Clinic'),
(80026, 'Pharmacy'),
(80027, 'Psychiatric Facility'),
(80028, 'Public Health Office'),
(80030, 'Rehabilitation Center'),
(80034, 'Substance Abuse Facility'),
(82000, 'Public Attraction or Landmark Building'),
(82002, 'Amusement / Water Park'),
(82004, 'Arboretum / Botanical Garden'),
(82006, 'Auditorium / Concert Hall / Theater / Opera House'),
(82008, 'Campground'),
(82010, 'Cemetery'),
(82011, 'Community / Recreation Center'),
(82012, 'Convention Center'),
(82014, 'Fair / Exhibition / Rodeo Grounds'),
(82016, 'Golf Course'),
(82018, 'Historic Site / Point of Interest'),
(82020, 'House of Worship'),
(82022, 'Ice Arena'),
(82024, 'Library'),
(82026, 'Lighthouse / Light'),
(82028, 'Lookout Tower'),
(82030, 'Marina'),
(82032, 'Museum'),
(82034, 'National Symbol / Monument'),
(82036, 'Observatory'),
(82038, 'Outdoor Theater / Amphitheater'),
(82040, 'Picnic Area'),
(82042, 'Racetrack / Dragstrip'),
(82044, 'Ski Area / Ski Resort'),
(82046, 'Sports Arena / Stadium'),
(82047, 'Trailhead'),
(82048, 'Visitor / Information Center'),
(82050, 'Zoo'),
(84000, 'Weather Facility or Structure'),
(84002, 'Warning Center'),
(84004, 'Weather Data Center'),
(84006, 'Weather Forecast Office'),
(84008, 'Weather Radar Site'),
(85000, 'Water Supply or Treatment Facility'),
(85001, 'Potable Water Facility'),
(85002, 'Public Water Supply Intake'),
(85004, 'Public Water Supply Well'),
(85006, 'Wastewater Treatment Plant'),
(85008, 'Water Pumping Station'),
(85010, 'Water System Control Facility'),
(85012, 'Water Tank'),
(85014, 'Water Tower'),
(85016, 'Water Treatment Facility'),
(88000, 'Information or Communication Facility'),
(88002, 'Communication Tower'),
(88004, 'Data Center'),
(88006, 'Internet DNS Location / Other Node'),
(88008, 'Internet Metro Area Exchange / Hub'),
(88010, 'Internet Service Provider'),
(88012, 'Radio / TV Broadcast Facility'),
(88014, 'Satellite Ground Station'),
(88016, 'Telephone Facility'),
(83000, 'Government or Military Facility'),
(83002, 'Bureau of Land Management Facility'),
(83004, 'US Capitol'),
(83006, 'State Capitol'),
(83008, 'US Supreme Court'),
(83010, 'State Supreme Court'),
(83011, 'Court House'),
(83012, 'Critical Federal Contractor Facility'),
(83014, 'Department of Energy Facility'),
(83016, 'Department of State Facility'),
(83018, 'Deparment of Motor Vehicle Facility'),
(83020, 'DoD / Military Facility'),
(83022, 'Governor Residence'),
(83024, 'Intelligence Facility'),
(83026, 'Local Government Facility'),
(83028, 'NASA Facility'),
(83030, 'National Guard Armory / Base'),
(83032, 'National Park Service Facility'),
(83034, 'State Government Facility'),
(83036, 'Tribal Government Facility'),
(83038, 'US Forest Service Facility'),
(83040, 'US Government Facility'),
(83042, 'White House'),
(83043, 'Department of Public Works'),
(83044, 'City / Town Hall');

ALTER TABLE usgs.struct_point
ADD CONSTRAINT fcodefk FOREIGN KEY (fcode)
REFERENCES usgs.fcode_choices (id) MATCH FULL;
