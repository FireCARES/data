CREATE MATERIALIZED VIEW nist.lr_mr_pred AS 
 SELECT tr.year,
    tr.tr10_fid AS geoid,
    tr.region,
    tr.state,
    g.id AS fd_id,
    'size_'::text || g.population_class::text AS fd_size,
    1 AS f_located,
        CASE
            WHEN acs."B25002_002" > 0 THEN acs."B01001_001"::double precision / acs."B25002_002"::double precision
            WHEN acs."B25002_002" = 0 AND acs."B01001_001" > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS ave_hh_sz,
    acs."B01001_001" AS pop,
    acs."B02001_003" AS black,
    acs."B02001_004" AS amer_es,
    acs."B02001_005" + acs."B02001_006" + acs."B02001_007" + acs."B02001_008" AS other,
    acs."B03003_003" AS hispanic,
    acs."B01001_002" AS males,
    acs."B01001_003" + acs."B01001_027" AS age_under5,
    acs."B01001_004" + acs."B01001_028" AS age_5_9,
    acs."B01001_005" + acs."B01001_029" AS age_10_14,
    acs."B01001_006" + acs."B01001_007" + acs."B01001_030" + acs."B01001_031" AS age_15_19,
    acs."B01001_008" + acs."B01001_009" + acs."B01001_010" + acs."B01001_032" + acs."B01001_033" + acs."B01001_034" AS age_20_24,
    acs."B01001_011" + acs."B01001_012" + acs."B01001_035" + acs."B01001_036" AS age_25_34,
    acs."B01001_013" + acs."B01001_014" + acs."B01001_037" + acs."B01001_038" AS age_35_44,
    acs."B01001_015" + acs."B01001_016" + acs."B01001_039" + acs."B01001_040" AS age_45_54,
    acs."B01001_017" + acs."B01001_018" + acs."B01001_019" + acs."B01001_041" + acs."B01001_042" + acs."B01001_043" AS age_55_64,
    acs."B01001_020" + acs."B01001_021" + acs."B01001_022" + acs."B01001_044" + acs."B01001_045" + acs."B01001_046" AS age_65_74,
    acs."B01001_023" + acs."B01001_024" + acs."B01001_047" + acs."B01001_048" AS age_75_84,
    acs."B01001_025" + acs."B01001_049" AS age_85_up,
    acs."B25002_001" AS hse_units,
    acs."B25002_003" AS vacant,
    acs."B25014_008" AS renter_occ,
    acs."B25014_005" + acs."B25014_006" + acs."B25014_007" + acs."B25014_011" + acs."B25014_012" + acs."B25014_013" AS crowded,
    acs."B25024_002" + acs."B25024_003" + acs."B25024_004" AS sfr,
    acs."B25024_007" + acs."B25024_008" + acs."B25024_009" AS units_10,
    acs."B25024_010" AS mh,
    acs."B25034_006" + acs."B25034_007" + acs."B25034_008" + acs."B25034_009" + acs."B25034_010" AS older,
    pcl.apts_n AS apt_parcels,
    pcl.mr_n AS mr_parcels,
    acs."B19013_001" AS inc_hh,
    acs."B25040_002" AS fuel_gas,
    acs."B25040_003" AS fuel_tank,
    acs."B25040_005" AS fuel_oil,
    acs."B25040_006" AS fuel_coal,
    acs."B25040_007" AS fuel_wood,
    acs."B25040_008" AS fuel_solar,
    acs."B25040_009" AS fuel_other,
    acs."B25040_010" AS fuel_none,
    svi.r_pl_themes AS svi,
    acs."B12001_001" - (acs."B12001_003" + acs."B12001_012") AS married,
    acs."B23025_005" AS unemployed,
    acs."B12001_007" AS nilf,
    sm.adult_smoke AS smoke_st,
    sc.smoking_pct AS smoke_cty
   FROM nist.tract_years tr
     LEFT JOIN nist.svi2010 svi ON tr.tr10_fid = ('14000US'::text || lpad(svi.fips::text, 11, '0'::text))
     LEFT JOIN firestation_firedepartment g ON tr.state::text = g.state::text AND tr.fdid = g.fdid::text
     LEFT JOIN nist.acs_est_new acs ON tr.tr10_fid = acs.geoid AND acs.year = 2015::double precision
     LEFT JOIN nist.sins sm ON tr.state::text = sm.postal_code AND sm.year = 2010
     LEFT JOIN nist.sins_county sc ON "substring"(tr.tr10_fid, 8, 5) = sc.fips
     LEFT JOIN nist.med_risk_parcel_info pcl ON tr.tr10_fid = ((('14000US'::text || pcl.state_code) || pcl.cnty_code) || pcl.tract)
  WHERE tr.year = 2016
WITH DATA;

ALTER TABLE nist.lr_mr_pred
    OWNER TO sgilbert;

COMMENT ON MATERIALIZED VIEW nist.lr_mr_pred
    IS 'This collects all the data needed to predict the number of fires etc. for low
and medium risk fires. There is (or should be) one entry per census tract. 

The main issue with this query is the use of hard-coded dates in a couple of
places in the JOIN clauses. As currently written, they will have to be updated 
periodically to keep the query up to date.';

GRANT ALL ON TABLE nist.lr_mr_pred TO firecares;
GRANT ALL ON TABLE nist.lr_mr_pred TO sgilbert;