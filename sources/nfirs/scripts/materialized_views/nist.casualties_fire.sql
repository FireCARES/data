CREATE MATERIALIZED VIEW nist.casualties_fire
TABLESPACE pg_default
AS
 WITH c AS (
         SELECT ffcasualty.state,
            ffcasualty.fdid,
            ffcasualty.inc_date,
            ffcasualty.inc_no,
            ffcasualty.exp_no,
            'ff'::text AS type,
            ffcasualty.ff_seq_no AS seq_no,
            ffcasualty.gender,
            ffcasualty.age,
            NULL::character varying AS race,
            NULL::character varying AS ethnicity,
                CASE
                    WHEN ffcasualty.severity::text = ANY (ARRAY['2'::character varying::text, '3'::character varying::text]) THEN '1'::character varying
                    WHEN ffcasualty.severity::text = '4'::text THEN '2'::character varying
                    WHEN ffcasualty.severity::text = '5'::text THEN '3'::character varying
                    WHEN ffcasualty.severity::text = '6'::text THEN '4'::character varying
                    WHEN ffcasualty.severity::text = '7'::text THEN '5'::character varying
                    ELSE ffcasualty.severity
                END AS sev
           FROM ffcasualty
          WHERE ffcasualty.severity::text <> '1'::text
        UNION
         SELECT civiliancasualty.state,
            civiliancasualty.fdid,
            civiliancasualty.inc_date,
            civiliancasualty.inc_no,
            civiliancasualty.exp_no,
            'civ'::text AS type,
            civiliancasualty.seq_number AS seq_no,
            civiliancasualty.gender,
            civiliancasualty.age,
            civiliancasualty.race,
            civiliancasualty.ethnicity,
            civiliancasualty.sev
           FROM civiliancasualty
        )
 SELECT c.state,
    c.fdid,
    c.inc_date,
    c.inc_no,
    c.exp_no,
    c.type,
    c.seq_no,
    date_part('year'::text, c.inc_date) AS year,
        CASE
            WHEN b.aid::text = ANY (ARRAY['3'::character varying::text, '4'::character varying::text]) THEN 'Y'::text
            ELSE 'N'::text
        END AS aid_flag,
    c.gender,
    c.age,
    c.race,
    c.ethnicity,
    c.sev,
        CASE
            WHEN b.prop_use::text ~~ '4%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS res,
        CASE
            WHEN (date_part('year'::text, b.inc_date) > 2001::double precision AND (b.inc_type::text = ANY (ARRAY['111'::character varying::text, '120'::character varying::text, '121'::character varying::text, '122'::character varying::text, '123'::character varying::text])) OR date_part('year'::text, b.inc_date) > 2001::double precision AND date_part('year'::text, b.inc_date) < 2008::double precision AND b.inc_type::text = '112'::text) AND (f.struc_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text])) OR ((b.inc_type::text = ANY (ARRAY['113'::character varying::text, '114'::character varying::text, '115'::character varying::text, '116'::character varying::text, '117'::character varying::text, '118'::character varying::text])) OR b.inc_type::text = '110'::text AND date_part('year'::text, b.inc_date) < 2009::double precision) AND ((f.struc_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text])) OR f.struc_type IS NULL) THEN 'Y'::text
            ELSE 'N'::text
        END AS struc,
        CASE
            WHEN f.state IS NULL THEN 'N'::text
            ELSE 'Y'::text
        END AS module,
        CASE
            WHEN b.prop_use::text = '419'::text OR b.prop_use::text ~~ '9%'::text THEN 'Low Risk'::text
            WHEN (b.prop_use::text <> ALL (ARRAY['419'::character varying::text, '644'::character varying::text, '645'::character varying::text])) AND ("substring"(b.prop_use::text, 1, 1) = ANY (ARRAY['4'::text, '5'::text, '6'::text, '7'::text, '8'::text])) AND (f.bldg_above IS NULL OR f.bldg_above::integer < 7) THEN 'Med Risk'::text
            ELSE 'High Risk'::text
        END AS risk,
        CASE
            WHEN a.bkgpidfp10 IS NULL THEN NULL::text
            ELSE '14000US'::text || "substring"(a.bkgpidfp10::text, 1, 11)
        END AS geoid,
    a.geom
   FROM c
     JOIN basicincident b USING (state, fdid, inc_date, inc_no, exp_no)
     LEFT JOIN incidentaddress a USING (state, fdid, inc_date, inc_no, exp_no)
     LEFT JOIN fireincident f USING (state, fdid, inc_date, inc_no, exp_no)
  WHERE b.inc_type::text ~~ '1%'::text OR f.state IS NOT NULL
WITH DATA;

ALTER TABLE nist.casualties_fire
    OWNER TO sgilbert;

GRANT ALL ON TABLE nist.casualties_fire TO firecares;
GRANT ALL ON TABLE nist.casualties_fire TO sgilbert;