CREATE MATERIALIZED VIEW nist.dept_incidents
AS
 WITH t0 AS (
         SELECT b.state,
            b.fdid,
            date_part('year'::text, b.inc_date) AS year,
                CASE
                    WHEN a.geom IS NOT NULL THEN 1
                    ELSE 0
                END AS located,
                CASE
                    WHEN "substring"(b.inc_type::text, 1, 1) = '1'::text OR f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS fire,
                CASE
                    WHEN "substring"(b.inc_type::text, 1, 1) = '3'::text THEN 1
                    ELSE 0
                END AS ems,
                CASE
                    WHEN "substring"(b.inc_type::text, 1, 1) = ANY (ARRAY['2'::text, '4'::text, '8'::text]) THEN 1
                    ELSE 0
                END AS hazard,
                CASE
                    WHEN "substring"(b.inc_type::text, 1, 1) = ANY (ARRAY['5'::text, '6'::text, '7'::text, '9'::text]) THEN 1
                    ELSE 0
                END AS svc,
                CASE
                    WHEN b.version::numeric(4,1) = 5.0::numeric(4,1) THEN 1
                    ELSE 0
                END AS version5,
                CASE
                    WHEN b.aid::text = ANY (ARRAY['3'::text, '4'::text]) THEN 0
                    ELSE 1
                END AS aid,
                CASE
                    WHEN (date_part('year'::text, b.inc_date) > 2001::double precision AND (b.inc_type::text = ANY (ARRAY['111'::text, '120'::text, '121'::text, '122'::text, '123'::text])) OR date_part('year'::text, b.inc_date) > 2001::double precision AND date_part('year'::text, b.inc_date) < 2008::double precision AND b.inc_type::text = '112'::text) AND (f.struc_type::text = ANY (ARRAY['1'::text, '2'::text])) OR ((b.inc_type::text = ANY (ARRAY['113'::text, '114'::text, '115'::text, '116'::text, '117'::text, '118'::text])) OR b.inc_type::text = '110'::text AND date_part('year'::text, b.inc_date) < 2009::double precision) AND ((f.struc_type::text = ANY (ARRAY['1'::text, '2'::text])) OR f.struc_type IS NULL) THEN 1
                    ELSE 0
                END AS struc,
                CASE
                    WHEN f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS module,
                CASE
                    WHEN f.not_res::text = 'N'::text OR b.prop_use::text ~~ '4%'::text THEN 1
                    ELSE 0
                END AS res,
                CASE
                    WHEN b.prop_use::text = '419'::text OR b.prop_use::text ~~ '9%'::text THEN 1
                    ELSE 0
                END AS lr,
            b.ff_inj,
            b.oth_inj,
            b.ff_death,
            b.oth_death
           FROM basicincident b
             LEFT JOIN incidentaddress a USING (state, fdid, inc_date, inc_no, exp_no)
             LEFT JOIN fireincident f USING (state, fdid, inc_date, inc_no, exp_no)
        ), t AS (
         SELECT t0.state,
            t0.fdid,
            t0.year,
            sum(t0.aid) AS incidents,
            sum(t0.aid * t0.located) AS incidents_loc,
            sum(t0.aid * t0.version5) AS v5_incidents,
            sum(t0.aid * t0.version5 * t0.located) AS v5_incidents_loc,
            sum(t0.aid * t0.fire) AS fires,
            sum(t0.aid * t0.located * t0.fire) AS fires_loc,
            sum(t0.aid * t0.ems) AS ems,
            sum(t0.aid * t0.located * t0.ems) AS ems_loc,
            sum(t0.aid * t0.hazard) AS hazard,
            sum(t0.aid * t0.located * t0.hazard) AS hazard_loc,
            sum(t0.aid * t0.svc) AS svc,
            sum(t0.aid * t0.located * t0.svc) AS svc_loc,
            sum(t0.aid * t0.module) AS mod_fires,
            sum(t0.aid * t0.located * t0.module) AS mod_fires_loc,
            sum(t0.aid * t0.struc) AS struc_fires,
            sum(t0.aid * t0.located * t0.struc) AS struc_fires_loc,
            sum(t0.aid * t0.res * t0.struc) AS res_fires,
            sum(t0.aid * t0.located * t0.res * t0.struc) AS res_fires_loc,
            sum(t0.aid * t0.lr * t0.struc) AS lr_fires,
            sum(t0.aid * t0.located * t0.lr * t0.struc) AS lr_fires_loc,
            sum(t0.ff_inj + t0.oth_inj * t0.aid) AS injuries,
            sum(t0.located * (t0.ff_inj + t0.oth_inj * t0.aid)) AS injuries_loc,
            sum(t0.ff_death + t0.oth_death * t0.aid) AS deaths,
            sum(t0.located * (t0.ff_death + t0.oth_death * t0.aid)) AS deaths_loc
           FROM t0
          GROUP BY t0.state, t0.fdid, t0.year
        ), e AS (
         SELECT ems.state,
            ems.fdid,
            date_part('year'::text, ems.inc_date) AS year,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = '1'::text THEN 1
                    ELSE 0
                END) AS fire_calls,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = '1'::text AND e_l.geom IS NOT NULL THEN 1
                    ELSE 0
                END) AS fire_calls_loc,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = '3'::text THEN 1
                    ELSE 0
                END) AS ems_calls,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = '3'::text AND e_l.geom IS NOT NULL THEN 1
                    ELSE 0
                END) AS ems_calls_loc,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = ANY (ARRAY['2'::text, '4'::text, '8'::text]) THEN 1
                    ELSE 0
                END) AS hazard_calls,
            sum(
                CASE
                    WHEN ("substring"(ems.inc_type::text, 1, 1) = ANY (ARRAY['2'::text, '4'::text, '8'::text])) AND e_l.geom IS NOT NULL THEN 1
                    ELSE 0
                END) AS hazard_calls_loc,
            sum(
                CASE
                    WHEN "substring"(ems.inc_type::text, 1, 1) = ANY (ARRAY['5'::text, '6'::text, '7'::text, '9'::text]) THEN 1
                    ELSE 0
                END) AS svc_calls,
            sum(
                CASE
                    WHEN ("substring"(ems.inc_type::text, 1, 1) = ANY (ARRAY['5'::text, '6'::text, '7'::text, '9'::text])) AND e_l.geom IS NOT NULL THEN 1
                    ELSE 0
                END) AS svc_calls_loc,
            sum(
                CASE
                    WHEN e_l.geom IS NULL THEN 0
                    ELSE 1
                END) AS calls_loc
           FROM ems.basicincident ems
             LEFT JOIN ems.incidentaddress e_l USING (state, fdid, inc_date, inc_no, exp_no)
          GROUP BY ems.state, ems.fdid, (date_part('year'::text, ems.inc_date))
        )
 SELECT
        CASE
            WHEN t.state IS NULL THEN e.state
            ELSE t.state
        END AS state,
        CASE
            WHEN t.fdid IS NULL THEN e.fdid
            ELSE t.fdid
        END AS fdid,
        CASE
            WHEN t.year IS NULL THEN e.year
            ELSE t.year
        END AS year,
    t.incidents,
    t.incidents_loc,
    t.v5_incidents,
    t.v5_incidents_loc,
    t.fires,
    t.fires_loc,
    t.ems,
    t.ems_loc,
    t.hazard,
    t.hazard_loc,
    t.svc,
    t.svc_loc,
    t.mod_fires,
    t.mod_fires_loc,
    t.struc_fires,
    t.struc_fires_loc,
    t.res_fires,
    t.res_fires_loc,
    t.lr_fires,
    t.lr_fires_loc,
    t.injuries,
    t.injuries_loc,
    t.deaths,
    t.deaths_loc,
    e.fire_calls,
    e.fire_calls_loc,
    e.ems_calls,
    e.ems_calls_loc,
    e.hazard_calls,
    e.hazard_calls_loc,
    e.svc_calls,
    e.svc_calls_loc
   FROM t
     FULL JOIN e ON t.state::text = e.state::text AND ltrim(t.fdid::text, '0'::text) = ltrim(e.fdid::text, '0'::text) AND t.year = e.year
WITH DATA;

ALTER TABLE nist.dept_incidents
    OWNER TO sgilbert;

COMMENT ON MATERIALIZED VIEW nist.dept_incidents
    IS 'Its purpose is to provide the data needed to correct for geolocation errors.
Note this view needs to be refreshed whenever new NFIRS data is added or when records 
are geolocated. (REFRESH MATERIALIZED VIEW nist.dept_incidents;)

This version incorporates information breaking down the types of calls.

Here INCIDENTS screen out mutual aid calls while CALLS includes them. 

The field CALLS in the previous version is now called EMS_CALLS. 

This query, as written, will take a long time to run.

Eventually, I want this version of this table to replace the earlier versions.';

GRANT ALL ON TABLE nist.dept_incidents TO firecares;
GRANT ALL ON TABLE nist.dept_incidents TO sgilbert;