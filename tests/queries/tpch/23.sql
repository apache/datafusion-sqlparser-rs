-- using default substitutions


with productivity as (select distinct
   o.owner_id,
   u.name,
   iff(
       u.segment_c = 'Enterprise',
       iff(
           u.geo_c = 'EMEA',
           'EMEA',
           iff(
               u.geo_c = 'APAC',
               'APAC',
               'Enterprise'
           )
       ),
       u.segment_c
   ) as segment_c ,
   start_date_c,
   iff(termination_date_c is null,
       date_trunc(month,current_date),
       dateadd(month, 1, date_trunc(month,termination_date_c))
   ) as last_month,
   date_trunc(month, o.close_date)::date as cohort_month,
   case
       when SEGMENT_C = 'Corporate'
            then 'None'
       when u.region_c = 'Northeast' or u.region_c = 'NY Metro' and u.segment_c != 'Corporate'
           then 'Northeast'
       when u.region_c = 'West' or u.region_c = 'Northern California' or u.region_c = 'PNW'
           then 'West'
       when u.region_c = 'Central' or u.region_c = 'North Central' or u.region_c = 'TOLA' or u.region_c = 'Midwest'
           then 'Central'
        when u.region_c = 'Southwest' or u.region_c = 'NY Rockies' or u.region_c = 'Southern California'
            then 'Southwest'
        when u.region_c = 'Southeast' or u.region_c = 'Philly Metro' or u.region_c = 'DMV'
            then 'Southeast'
        when u.geo_c = 'EMEA'
              then u.region_c
        when u.segment_c = 'APAC'
              then u.segment_c
        when segment_c = 'Majors'
              then u.region_c
        else 'None'
    end as region,
   sum(
       iff(
         forecast_acv_c is not null,
         iff(
           base_renewal_acv_c is null,
           iff(
               forecast_acv_c - 0 < 1,
               0,
               forecast_acv_c - 0
           ),
           iff(
               forecast_acv_c - base_renewal_acv_c < 1,
               0,
               forecast_acv_c - base_renewal_acv_c
          )
        ),
      0
      )
   ) as bookings
from fivetran.salesforce.opportunity as o
left join  fivetran.salesforce.user as u on u.id = owner_id
left join fivetran.salesforce.account as a on a.id = o.account_id
where stage_name = 'Closed Won' and close_date >= '2015-02-01' and start_date_c < cohort_month and u.function_c = 'Account Executive' and start_date_c >= '2015-02-01'
group by 1,2,3,4,5,6,7
order by cohort_month asc),
missing_months as (
 select distinct date_trunc(month,_date)::date as cohort_month, owner_id, name, p.segment_c, iff(p.region is null, 'None', p.region) as region, p.start_date_c as sd
 from snowhouse.utils.calendar as c
 JOIN productivity as  p
   ON date_trunc(month,_date)::date BETWEEN date_trunc(month,p.start_date_c) AND coalesce(DATEADD(month,0,p.last_month),p.start_date_c)
 where _date > '2015-02-01' and _date <= current_date and last_month >= cohort_month order by owner_id desc, cohort_month asc
) ,
reps_padded_with_month as (
 select
   m.*,
   iff(p.bookings is null,
       0,
       p.bookings
      ) as bookings
from missing_months as m
left join productivity as p
on p.owner_id = m.owner_id and m.cohort_month = p.cohort_month and m.region = p.region
),
pre_pivot_work as (select
   row_number() over (partition by owner_id order by cohort_month asc) as active_month,
   owner_id,name, region,segment_c, bookings, sd
from reps_padded_with_month),
rolling_sum as (
   select
       owner_id,
       name,
       region,
       segment_c,
       active_month,
       last_value(active_month) over (partition by owner_id order by active_month asc) as tenure,
       sd,
       sum(bookings) over (partition by owner_id order by active_month asc) as p
       from pre_pivot_work
),
ltm as (select
       owner_id,
       name,
       region,
       segment_c,
       active_month,
       last_value(active_month) over (partition by owner_id order by active_month asc) as tenure,
       sd,
       iff(
           active_month >= 12,
           sum(bookings) over (partition by owner_id order by active_month asc rows 11 PRECEDING),
           sum(bookings) over (partition by owner_id order by active_month)
       ) as p
from pre_pivot_work),
years_included as (select *, date_trunc(year, sd) as start_year from ltm),

all_reps as (
select a.*, a.p as growth_bookings,
   max(a.p) over (partition by a.name) as max_growth,
   iff(
       a.p <= 0,
       (a.p - b.p) / 1,
       iff(
           b.p <= 0,
           a.p-a.p /1,
           (a.p - b.p) / a.p
        )
   ) as rate_of_change
   from years_included as a
   left join years_included as b on b.owner_id = a.owner_id and b.active_month = a.active_month-1
   where a.active_month <= 24
   order by name, active_month),

  percents as (select
       segment_c,
       PERCENTILE_CONT (.80) WITHIN GROUP (ORDER BY max_growth) as p80
   from all_reps
   group by segment_c),

    percents2 as (select
       segment_c,
       region,
       PERCENTILE_CONT (.8) WITHIN GROUP (ORDER BY max_growth) as p80
   from all_reps
   group by segment_c, region),

   temp as (select distinct a.*,
   iff(
       max_growth >= p.p80,
       1,
       0
   ) as outlier_by_segment
   from all_reps as a
       left join percents as p
           on p.segment_c = a.segment_c)

select a.*,
iff (
       max_growth >= l.p80,
       1,
       0
   ) as outlier_by_region
from temp as a
left join percents2 as l
on a.region = l.region and a.segment_c = l.segment_c
order by name asc, active_month asc;
