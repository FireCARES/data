CREATE OR REPLACE VIEW nist.ems_500_pred AS 
 SELECT year,
    geoid,
    region,
    state,
    fdid,
    fc_dept_id,
    fd_size,
    cluster,
    NULL::double precision AS ems,
    ave_hh_sz,
    pop,
    black,
    amer_es,
    other,
    hispanic,
    males,
    age_under5,
    age_5_9,
    age_10_14,
    age_15_19,
    age_20_24,
    age_25_34,
    age_35_44,
    age_45_54,
    age_55_64,
    age_65_74,
    age_75_84,
    age_85_up,
    hse_units,
    vacant,
    renter_occ,
    crowded,
    sfr,
    units_10,
    mh,
    older,
    inc_hh,
    svi,
    married,
    unemployed,
    nilf,
    fuel_gas,
    fuel_tank,
    fuel_oil,
    fuel_coal,
    fuel_wood,
    fuel_solar,
    fuel_other,
    fuel_none,
    arthritis,
    bphigh,
    cancer,
    casthma,
    chd,
    copd,
    diabetes,
    highchol,
    kidney,
    mhlth,
    phlth,
    stroke,
    teethlost,
    access2,
    bpmed,
    checkup,
    cholscreen,
    colon_screen,
    corem,
    corew,
    dental,
    mammouse,
    paptest,
    binge,
    csmoking,
    lpa,
    obesity,
    sleep
   FROM nist.ems_table_500
  WHERE year = (( SELECT max(e1.year) AS max
           FROM nist.ems_table_500 e1
          WHERE e1.fc_dept_id IS NOT NULL));

ALTER TABLE nist.ems_500_pred
  OWNER TO sgilbert;