CREATE MATERIALIZED VIEW nist.county_clustering
AS
 WITH i AS (
         SELECT "substring"(c_1.geoid, 8, 5) AS geoid,
            count(*) AS n
           FROM ems.incidentaddress a
             JOIN nist.census_tract_locs_swg c_1 ON st_within(a.geom, c_1.geom)
          GROUP BY ("substring"(c_1.geoid, 8, 5))
        ), t1 AS (
         SELECT DISTINCT "substring"(ems_table_cnty.geoid, 8, 5) AS geoid,
            ems_table_cnty.years_lost,
            ems_table_cnty.poor_health,
            ems_table_cnty.days_pr_hlth,
            ems_table_cnty.days_pr_mntl,
            ems_table_cnty.low_birthwt,
            ems_table_cnty.csmoking,
            ems_table_cnty.obesity,
            ems_table_cnty.food_ndx,
            ems_table_cnty.lpa,
            ems_table_cnty.exercise_place,
            ems_table_cnty.binge,
            ems_table_cnty.dui,
            ems_table_cnty.stds,
            ems_table_cnty.teen_births,
            ems_table_cnty.access2,
            ems_table_cnty.physicians,
            ems_table_cnty.dentists,
            ems_table_cnty.shrinks,
            ems_table_cnty.wrong_hosp,
            ems_table_cnty.diabetic_scrn,
            ems_table_cnty.mammography,
            ems_table_cnty.high_school,
            ems_table_cnty.college,
            ems_table_cnty.child_pov,
            ems_table_cnty.inequality,
            ems_table_cnty.child_sngl_prnt,
            ems_table_cnty.social,
            ems_table_cnty.violent,
            ems_table_cnty.injury_dths,
            ems_table_cnty.pm10,
            ems_table_cnty.house_probs,
            ems_table_cnty.drive_alone,
            ems_table_cnty.long_commute,
            ems_table_cnty.early_mortality,
            ems_table_cnty.child_mortality,
            ems_table_cnty.infant_death,
            ems_table_cnty.phys_distress,
            ems_table_cnty.mntl_distress,
            ems_table_cnty.diabetes,
            ems_table_cnty.hiv,
            ems_table_cnty.food_insecurity,
            ems_table_cnty.no_healthy_food,
            ems_table_cnty.drug_overdose,
            ems_table_cnty.mv_deaths,
            ems_table_cnty.lack_sleep,
            ems_table_cnty.uninsured_adult,
            ems_table_cnty.uninsured_child,
            ems_table_cnty.hlth_cost,
            ems_table_cnty.nurses,
            ems_table_cnty.free_lunch,
            ems_table_cnty.segregation1,
            ems_table_cnty.segregation2,
            ems_table_cnty.homicide,
            ems_table_cnty.rural
           FROM nist.ems_table_cnty
        ), t2 AS (
         SELECT "substring"(ems_table_cnty.geoid, 8, 5) AS geoid,
            sum(ems_table_cnty.pop) AS pop,
            sum(ems_table_cnty.black) AS black,
            sum(ems_table_cnty.amer_es) AS amer_es,
            sum(ems_table_cnty.other) AS other,
            sum(ems_table_cnty.hispanic) AS hispanic,
            sum(ems_table_cnty.males) AS males,
            sum(ems_table_cnty.age_under5) AS age_under5,
            sum(ems_table_cnty.age_5_9) AS age_5_9,
            sum(ems_table_cnty.age_10_14) AS age_10_14,
            sum(ems_table_cnty.age_15_19) AS age_15_19,
            sum(ems_table_cnty.age_20_24) AS age_20_24,
            sum(ems_table_cnty.age_25_34) AS age_25_34,
            sum(ems_table_cnty.age_35_44) AS age_35_44,
            sum(ems_table_cnty.age_45_54) AS age_45_54,
            sum(ems_table_cnty.age_55_64) AS age_55_64,
            sum(ems_table_cnty.age_65_74) AS age_65_74,
            sum(ems_table_cnty.age_75_84) AS age_75_84,
            sum(ems_table_cnty.age_85_up) AS age_85_up,
            sum(ems_table_cnty.hse_units) AS hse_units,
            sum(ems_table_cnty.vacant) AS vacant,
            sum(ems_table_cnty.renter_occ) AS renter_occ,
            sum(ems_table_cnty.crowded) AS crowded,
            sum(ems_table_cnty.sfr) AS sfr,
            sum(ems_table_cnty.units_10) AS units_10,
            sum(ems_table_cnty.mh) AS mh,
            sum(ems_table_cnty.older) AS older,
            sum(ems_table_cnty.married) AS married,
            sum(ems_table_cnty.unemployed) AS unemployed,
            sum(ems_table_cnty.nilf) AS nilf,
            sum(ems_table_cnty.fuel_gas) AS fuel_gas,
            sum(ems_table_cnty.fuel_tank) AS fuel_tank,
            sum(ems_table_cnty.fuel_oil) AS fuel_oil,
            sum(ems_table_cnty.fuel_coal) AS fuel_coal,
            sum(ems_table_cnty.fuel_wood) AS fuel_wood,
            sum(ems_table_cnty.fuel_solar) AS fuel_solar,
            sum(ems_table_cnty.fuel_other) AS fuel_other,
            sum(ems_table_cnty.fuel_none) AS fuel_none
           FROM nist.ems_table_cnty
          GROUP BY ("substring"(ems_table_cnty.geoid, 8, 5))
        ), t3 AS (
         SELECT "substring"(ems_table_cnty.geoid, 8, 5) AS geoid,
            sum(ems_table_cnty.hse_units::double precision * ems_table_cnty.ave_hh_sz) AS ave_hh_size,
            sum(
                CASE
                    WHEN ems_table_cnty.inc_hh = 'null'::text OR ems_table_cnty.inc_hh = ''::text THEN NULL::double precision
                    ELSE ems_table_cnty.hse_units::double precision * ems_table_cnty.inc_hh::double precision
                END) AS inc_hh
           FROM nist.ems_table_cnty
          GROUP BY ("substring"(ems_table_cnty.geoid, 8, 5))
        ), c AS (
         SELECT us_counties.geoid,
            st_centroid(us_counties.wkb_geometry) AS geom,
            st_x(st_centroid(us_counties.wkb_geometry)) AS x,
            st_y(st_centroid(us_counties.wkb_geometry)) AS y
           FROM us_counties
        )
 SELECT c.geoid,
    c.x,
    c.y,
    i.n AS calls,
    t2.pop,
    t2.black,
    t2.amer_es,
    t2.other,
    t2.hispanic,
    t2.males,
    t2.age_under5,
    t2.age_5_9,
    t2.age_10_14,
    t2.age_15_19,
    t2.age_20_24,
    t2.age_25_34,
    t2.age_35_44,
    t2.age_45_54,
    t2.age_55_64,
    t2.age_65_74,
    t2.age_75_84,
    t2.age_85_up,
    t2.hse_units,
    t2.vacant,
    t2.renter_occ,
    t2.crowded,
    t2.sfr,
    t2.units_10,
    t2.mh,
    t2.older,
    t2.married,
    t2.unemployed,
    t2.nilf,
    t2.fuel_gas,
    t2.fuel_tank,
    t2.fuel_oil,
    t2.fuel_coal,
    t2.fuel_wood,
    t2.fuel_solar,
    t2.fuel_other,
    t2.fuel_none AS years_lost,
    t1.poor_health,
    t1.days_pr_hlth,
    t1.days_pr_mntl,
    t1.low_birthwt,
    t1.csmoking,
    t1.obesity,
    t1.food_ndx,
    t1.lpa,
    t1.exercise_place,
    t1.binge,
    t1.dui,
    t1.stds,
    t1.teen_births,
    t1.access2,
    t1.physicians,
    t1.dentists,
    t1.shrinks,
    t1.wrong_hosp,
    t1.diabetic_scrn,
    t1.mammography,
    t1.high_school,
    t1.college,
    t1.child_pov,
    t1.inequality,
    t1.child_sngl_prnt,
    t1.social,
    t1.violent,
    t1.injury_dths,
    t1.pm10,
    t1.house_probs,
    t1.drive_alone,
    t1.long_commute,
    t1.early_mortality,
    t1.child_mortality,
    t1.infant_death,
    t1.phys_distress,
    t1.mntl_distress,
    t1.diabetes,
    t1.hiv,
    t1.food_insecurity,
    t1.no_healthy_food,
    t1.drug_overdose,
    t1.mv_deaths,
    t1.lack_sleep,
    t1.uninsured_adult,
    t1.uninsured_child,
    t1.hlth_cost,
    t1.nurses,
    t1.free_lunch,
    t1.segregation1,
    t1.segregation2,
    t1.homicide,
    t1.rural,
    t3.ave_hh_size / t2.hse_units::double precision AS ave_hh_size,
    t3.inc_hh / t2.hse_units::double precision AS inc_hh
   FROM c
     LEFT JOIN t1 USING (geoid)
     LEFT JOIN t2 USING (geoid)
     LEFT JOIN t3 USING (geoid)
     LEFT JOIN i USING (geoid)
WITH DATA;

ALTER TABLE nist.county_clustering
    OWNER TO sgilbert;

COMMENT ON MATERIALIZED VIEW nist.county_clustering
    IS 'This summarizes the EMS (county) predictors by rolling them up to the county level.
The information in subquery t1 is largely from www.countyhealthrankings.org and is
already at the county level. So they simply need to be collected. The information in subquery
t2 is at the census tract level, and needs to be summed up to the county level. The information
in subquery t3 is averaged per household per census tract. To roll it up to the county level 
requires a weighted average by number of households. The terms x and y are approximate centroids
on the county, and are used to provide a rough regionalization to the cluster algorithm.';

GRANT ALL ON TABLE nist.county_clustering TO firecares;
GRANT ALL ON TABLE nist.county_clustering TO sgilbert;