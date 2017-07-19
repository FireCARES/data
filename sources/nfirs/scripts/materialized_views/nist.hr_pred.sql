CREATE MATERIALIZED VIEW nist.hr_pred AS 
 WITH f AS (
         SELECT DISTINCT h.parcel_id,
            h.geoid,
            h.geoid_source,
            h.res_corelogic,
            h.res_other,
            h.bld_units,
            h.hr_floors,
            h.eff_yr,
            h.risk_class
           FROM nist.hr_parcel_fires h
        )
 SELECT t.year,
    f.parcel_id,
    f.geoid,
    f.geoid_source,
    t.region,
    t.state,
    t.fc_dept_id AS fd_id,
    'size_'::text || g.population_class::text AS fd_size,
    f.res_corelogic,
    f.res_other,
    f.bld_units,
    f.hr_floors,
    f.eff_yr,
    f.risk_class,
    NULL::double precision AS fires,
    NULL::double precision AS size_1,
    NULL::double precision AS size_2,
    NULL::double precision AS size_3,
    NULL::double precision AS deaths,
    NULL::double precision AS injuries,
        CASE
            WHEN acs."B25002_002E" > 0 THEN acs."B01001_001E"::double precision / acs."B25002_002E"::double precision
            WHEN acs."B25002_002E" = 0 AND acs."B01001_001E" > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS ave_hh_sz,
    acs."B01001_001E" AS pop,
    acs."B02001_003E" AS black,
    acs."B02001_004E" AS amer_es,
    acs."B02001_005E" + acs."B02001_006E" + acs."B02001_007E" + acs."B02001_008E" AS other,
    acs."B03003_003E" AS hispanic,
    acs."B01001_002E" AS males,
    acs."B01001_003E" + acs."B01001_027E" AS age_under5,
    acs."B01001_004E" + acs."B01001_028E" AS age_5_9,
    acs."B01001_005E" + acs."B01001_029E" AS age_10_14,
    acs."B01001_006E" + acs."B01001_007E" + acs."B01001_030E" + acs."B01001_031E" AS age_15_19,
    acs."B01001_008E" + acs."B01001_009E" + acs."B01001_010E" + acs."B01001_032E" + acs."B01001_033E" + acs."B01001_034E" AS age_20_24,
    acs."B01001_013E" + acs."B01001_014E" + acs."B01001_037E" + acs."B01001_038E" AS age_35_44,
    acs."B01001_015E" + acs."B01001_016E" + acs."B01001_039E" + acs."B01001_040E" AS age_45_54,
    acs."B01001_017E" + acs."B01001_018E" + acs."B01001_019E" + acs."B01001_041E" + acs."B01001_042E" + acs."B01001_043E" AS age_55_64,
    acs."B01001_020E" + acs."B01001_021E" + acs."B01001_022E" + acs."B01001_044E" + acs."B01001_045E" + acs."B01001_046E" AS age_65_74,
    acs."B01001_023E" + acs."B01001_024E" + acs."B01001_047E" + acs."B01001_048E" AS age_75_84,
    acs."B01001_025E" + acs."B01001_049E" AS age_85_up,
    acs."B25002_001E" AS hse_units,
    acs."B25002_003E" AS vacant,
    acs."B25014_008E" AS renter_occ,
    acs."B25014_005E" + acs."B25014_006E" + acs."B25014_007E" + acs."B25014_011E" + acs."B25014_012E" + acs."B25014_013E" AS crowded,
    acs."B25024_002E" + acs."B25024_003E" + acs."B25024_004E" AS sfr,
    acs."B25024_007E" + acs."B25024_008E" + acs."B25024_009E" AS units_10,
    acs."B25024_010E" AS mh,
    acs."B25034_006E" + acs."B25034_007E" + acs."B25034_008E" + acs."B25034_009E" + acs."B25034_010E" AS older,
    acs."B19013_001E" AS inc_hh,
    svi.r_pl_themes AS svi,
    acs."B12001_001" - (acs."B12001_003" + acs."B12001_012") AS married,
    acs."B23025_005" AS unemployed,
    acs."B12001_007" AS nilf,
    sm.adult_smoke AS smoke_st,
    sc.smoking_pct AS smoke_cty
   FROM f
     LEFT JOIN nist.tract_years t ON f.geoid = t.tr10_fid
     LEFT JOIN firestation_firedepartment g ON t.state::text = g.state::text AND t.fdid::text = g.fdid::text
     LEFT JOIN nist.svi2010 svi ON f.geoid = ('14000US'::text || svi.fips::text)
     LEFT JOIN nist.acs_est_new acs ON f.geoid = acs.geoid AND acs.year = 2015::double precision
     LEFT JOIN nist.sins sm ON t.state::text = sm.postal_code AND sm.year = 2010
     LEFT JOIN nist.sins_county sc ON "substring"(f.geoid, 8, 5) = sc.fips
  WHERE t.year = 2014
WITH DATA;

ALTER TABLE nist.hr_pred
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.hr_pred TO sgilbert;
GRANT SELECT ON TABLE nist.hr_pred TO firecares;
COMMENT ON MATERIALIZED VIEW nist.hr_pred
  IS 'This collects all the data needed to predict the number of fires etc. for high
risk fires. There is (or should be) one entry per high-risk parcel. 

The main issue with this query is the use of hard-coded dates in a couple of
places in the JOIN clauses. As currently written, they will have to be updated 
periodically to keep the query up to date.';