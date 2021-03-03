--Truncate table NYTimesData

-- Once the python code (NYTImesDataImport.pyndb ) is run and the data is pulled into the table run this code 
--for various breakdowns of the data.  County and state level aggregates, case and death density values as well 
--as growth rates are calculated below.  Some data manipulation has to be done to handle the fact that all 5 boroughs of 
-- NYC are reported as a single value (despite them each being there own county fips code.

select top 10 * from NYTimesData
select count(*) from nytimesdata
select max(date) from nytimesdata

update NYTimesData
set fips = '36999'
where county = 'New York City'

--ALTER TABLE NYTimesData
--ADD CountyPopulation int NULL;

--ALTER TABLE NYTimesData
--ADD CasesPer1000 numeric(6,2) NULL;

--ALTER TABLE NYTimesData
--ADD Deathsper1000 numeric(6,2) NULL;

-- get population info

  update NYTimesData
  set countypopulation = b.countypopulation2010
  from NYTimesData a inner join riskfactordata b
  on a.fips = b.countyfips


  -- calculate case and death rates
  update NYTimesData
  set Casesper1000 = round((cases * 1000.000) / countypopulation, 2)

  update NYTimesData
  set Deathsper1000 = round((deaths * 1000.000) / countypopulation, 2)

  -- which counties have the highest case density on most current date -- Data is one day behind the current date
  select * from NYTimesData
  where date = CONVERT(date, getdate() - 1)
  order by casesper1000 desc

  -- which counties have the highest death rate (deaths per 1000) on most current date
  select * from NYTimesData
  where date = CONVERT(date, getdate() - 1)
  order by deathsper1000 desc

  -- States with the highest case densities
  select state
	, date
	, sum(countypopulation) as Statepopulation
	, sum(cases) as StateCases
	, sum(deaths) as StateDeaths
	, CAST(round((sum(cases) * 1000.000) / sum(countypopulation), 2) as numeric(5,2)) as StateCasesper1000
	, CAST(round((sum(deaths) * 1000.000) / sum(countypopulation), 2) as numeric(5,2)) as StateDeathsper1000
  from NYTimesData
  where date = CONVERT(date, getdate() - 1)
  and fips is not null
--  and countypopulation < 100000
  group by state, date
  order by stateCasesper1000 desc

drop table #CountyStats

SELECT *
	, lag(cases, 7) over (partition by fips order by date) as CasesOneWeekAgo
	, cases - lag(cases, 7) over (partition by fips order by date) as WeeklyCaseIncrease
	, lag(Casesper1000, 7) over (partition by fips order by date) as CaseDensity1weekAgo
	, lag(cases, 30) over (partition by fips order by date) as CasesOneMonthAgo
	, cases - lag(cases, 30) over (partition by fips order by date) as MonthlyCaseIncrease
	, lag(Casesper1000, 30) over (partition by fips order by date) as CaseDensity1MonthAgo
	, CAST(ROUND((casesper1000 - lag(casesper1000, 7) over (partition by fips order by date ))* 1.00 / lag(IIF(casesper1000 = 0, NULL, casesPer1000),7) over (partition by fips order by date),3) as numeric(6,3)) as WeeklyCaseDensityGrowthRate
	, CAST(ROUND((casesper1000 - lag(casesper1000, 30) over (partition by fips order by date ))* 1.00 / lag(IIF(casesper1000 = 0, NULL, casesPer1000),30) over (partition by fips order by date),3) as numeric(8,3)) as MonthlyCaseDensityGrowthRate
	, lag(deaths, 7) over (partition by fips order by date) as DeathsOneWeekAgo
	, deaths - lag(deaths, 7) over (partition by fips order by date) as WeeklyDeathsIncrease
	, deaths - lag(deaths,30) over (partition by fips order by date) as MonthlyDeathsIncrease
	, lag(Deathsper1000, 7) over (partition by fips order by date) as DeathDensity1weekAgo
	, lag(deaths, 30) over (partition by fips order by date) as DeathsOneMonthAgo
	, lag(Deathsper1000, 30) over (partition by fips order by date) as DeathDensityOneMonthAgo	
	, CAST(ROUND((Deathsper1000 - lag(Deathsper1000, 7) over (partition by fips order by date ))* 1.00 / lag(IIF(Deathsper1000 = 0, NULL, DeathsPer1000),7) over (partition by fips order by date),3) as numeric(6,3)) as WeeklyDeathDensityGrowthRate
	, CAST(ROUND((Deathsper1000 - lag(Deathsper1000, 30) over (partition by fips order by date ))* 1.00 / lag(IIF(Deathsper1000 = 0, NULL, DeathsPer1000),30) over (partition by fips order by date),3) as numeric(6,3)) as MonthlyDeathDensityGrowthRate
INTO #CountyStats
  FROM [GISData].[dbo].[NYTimesData]
  where fips is not NULL
  order by 5,2

drop table CountyVirusData_mapping


select a.*, b.shape
into CountyVirusData_mapping
from #CountyStats a inner join uscounties b
on a.fips = b.fips

--  Hot Counties Cases - Weekly
select county, state, fips, countypopulation, cases
, casesper1000, casesOneWeekAgo, CasesOneMonthAgo, CaseDensity1weekAgo, CaseDensity1MonthAgo, WeeklyCaseDensityGrowthRate, MonthlyCaseDensityGrowthRate 
from #CountyStats
where date = CONVERT(date, getdate() - 1)
and Cases >= 50
--order by county 
order by WeeklyCaseDensityGrowthRate desc

--  Hot Counties Cases - Monthly
select county, state, fips, cases
, casesper1000, casesOneWeekAgo, CasesOneMonthAgo, CaseDensity1weekAgo, CaseDensity1MonthAgo, WeeklyCaseDensityGrowthRate, MonthlyCaseDensityGrowthRate 
from #CountyStats
where date = CONVERT(date, getdate() - 1)
and CasesOneMonthAgo > 5
order by MonthlyCaseDensityGrowthRate desc

-- Hot County Deaths - Weekly
select county, state, fips, cases, deaths
, casesper1000, deathsper1000, DeathsOneWeekAgo, DeathsOneMonthAgo, DeathDensity1weekAgo, DeathDensityOneMonthAgo, WeeklyDeathDensityGrowthRate, MonthlyDeathDensityGrowthRate
from #CountyStats
where date = CONVERT(date, getdate() - 1)
and DeathsOneWeekAgo > 5
order by WeeklyDeathDensityGrowthRate desc


-- Hot County Deaths - Monthly
select county, state, fips, cases, deaths
, casesper1000, deathsper1000, DeathsOneWeekAgo, DeathsOneMonthAgo, DeathDensity1weekAgo, DeathDensityOneMonthAgo, WeeklyDeathDensityGrowthRate, MonthlyDeathDensityGrowthRate
from #CountyStats
where date = CONVERT(date, getdate() - 1)
and DeathsOneMonthAgo >= 5
order by MonthlyDeathDensityGrowthRate desc


drop table #StateRawNumbers

select state
	, date
	, sum(CountyPopulation) as StatePopulation
	, sum(cases) as StateCases
	, sum(CasesOneWeekAgo) as StateCasesOneWeekAgo
	, sum(CasesOneMonthAgo) as StateCasesOneMonthAgo
	, sum(WeeklyCaseIncrease) as StateWeeklyCaseIncrease
	, sum(MonthlyCaseIncrease) as StateMonthlyCaseIncrease
	, sum(deaths) as StateDeathsCurrent
	, sum(DeathsOneWeekAgo) as StateDeathsOneWeekAgo
	, sum(DeathsOneMonthAgo) as StateDeathsOneMonthAgo
	, sum(WeeklyDeathsIncrease) as StateWeeklyDeathsIncrease
	, sum(monthlydeathsIncrease) as StateMonthlyDeathsIncrease
INTO #StateRawNumbers
from #CountyStats
--where date = '2020-05-14'
group by state, date
order by state, date desc

drop table #StateFinalRawNumbers

select *
	, CAST(round((StateCases * 1000.000) / StatePopulation, 3) as numeric(6,3)) as StateCasesPer1000_Current
	, CAST(round((StateCasesOneWeekAgo * 1000.000) / StatePopulation, 3) as numeric(6,3)) as StateCasesPer1000_OneWeekAgo
	, CAST(Round((StateCasesOneMonthAgo * 1000.000)/ StatePopulation, 3) as numeric(6,3)) as StateCasesPer1000_OneMonthAgo
	, CAST(ROUND((StateDeathsCurrent * 1000.000) / StatePopulation, 3) as numeric(8,3)) as StateDeathsPer1000_Current
	, CAST(ROUND((StateDeathsOneWeekAgo * 1000.000) / statepopulation, 3) as numeric(8,3)) as StateDeathsPer1000_OneWeekAgo
	, CAST(ROUND((StateDeathsOneMonthAgo * 1000.000) / StatePopulation, 3) as numeric(8,3)) as StateDeathsPer1000_OneMonthAgo
INTO #StateFinalRawNumbers
from #StateRawNumbers
order by state, date


drop table #StateAllNumbers

select *
	, CAST(ROUND((StateCasesPer1000_current - stateCasesPer1000_oneWeekAgo) / IIF(statecasesPer1000_oneWeekAgo = 0, NULL, stateCasesPer1000_oneWeekAgo),3) as numeric(7,3)) as StateCaseWeeklyGrowthRate
	, CAST(ROUND((StateCasesPer1000_current - stateCasesPer1000_oneMonthAgo) / IIF(statecasesPer1000_oneMonthAgo = 0, NULL, stateCasesPer1000_oneMonthAgo),3) as numeric(7,3)) as StateCaseMonthlyGrowthRate
	, CAST(ROUND((StateDeathsPer1000_current - StateDeathsPer1000_oneWeekAgo) / IIF(stateDeathsPer1000_oneWeekAgo = 0, NULL, StateDeathsPer1000_oneWeekAgo), 3) as numeric(9,3)) as StateDeathsWeeklyGrowthRate
	, CAST(ROUND((StateDeathsper1000_current - StateDeathsPer1000_oneMonthAgo) / IIF(StateDeathsPer1000_oneMonthAgo = 0, NULL, StateDeathsPer1000_oneMonthAgo), 3) as numeric(9,3)) as StateDeathsMonthlyGrowthRate
INTO #StateAllNumbers
FROM #StateFinalRawNumbers
--where date = '2020-05-14'
--and state = 'Alaska'
order by StateCaseWeeklyGrowthRate desc


select State
	, date
	, IIF(StateCaseWeeklyGrowthRate = NULL, NULL, rank() over (partition by date order by StateCaseWeeklyGrowthRate desc)) as RankByWeeklyCasesGrowthRate
	, IIF(StateDeathsWeeklyGrowthRate = NULL, NULL, rank() over (partition by date order by StateDeathsWeeklyGrowthRate desc)) as RankByWeeklyDeathsGrowthRate
	, IIF(StateCaseMonthlyGrowthRate = NULL, NULL, rank() over (partition by date order by StateCaseMonthlyGrowthRate desc)) as RankByMonthlyCasesGrowthRate
	, IIF(StateDeathsMonthlyGrowthRate = NULL, NULL, rank() over (partition by date order by StateDeathsMonthlyGrowthRate desc)) as RankByMonthlyDeathsGrowthRate
into #StateRanks
from #StateAllNumbers
where StateCaseWeeklyGrowthRate is not NULL and StateDeathsWeeklyGrowthRate is not NULL and stateCaseMonthlyGrowthRate is not null and stateDeathsMonthlyGrowthRate is not null
order by state, date desc

-- States by Case Weekly Growth Rate
select state, date, statepopulation, statecases, StateCasesOneWeekAgo, StateCaseWeeklyGrowthRate
from #StateAllNumbers
where date = CONVERT(date, getdate() - 1)
order by StateCaseWEEKLYGrowthRate desc

-- States by DeathWeeklyGrowthRate
select State, date, statepopulation, StateDeathsOneWeekAgo, StateDeathsCurrent, StateDeathsWeeklyGrowthRate
from #StateAllNumbers
where date = CONVERT(date, getdate() - 1)
order by StateDeathsWeeklyGrowthRate desc

-- States by Case Monthly Growth Rate
select state, date, statepopulation, statecases, StateCasesOneMonthAgo, StateCaseMonthlyGrowthRate
from #StateAllNumbers
where date = CONVERT(date, getdate() - 1)
order by StateCaseMonthlyGrowthRate desc

-- States by Death Montly Growth rate
select * from #StateAllNumbers
where date = CONVERT(date, getdate() - 1)
order by StateDeathsMonthlyGrowthRate desc


select *
from #stateRanks
where date = CONVERT(date, getdate() - 1)
order by RankByWeeklyCasesGrowthRate


select *
from #stateRanks
where state = 'Vermont'  -- Choose a state to see It's rank over time
order by date desc


-- For animation


--/****** Script for SelectTopNRows command from SSMS  ******/

--drop table US_CountyCoronavirusCaseStats_0917

--SELECT a.*, b.shape
--INTO US_CountyCoronavirusCaseStats_0917
--  FROM [GISData].[dbo].[NYTimesData] a left outer join dbo.usCounties b
--  on a.fips = b.FIPS
--  where a.fips is not null



--  -- growth rates for animation.

--  select date, county, state, fips, cases
--  into #CurrentCaseTotals
--  from #countyStats
--  where date = '2020-07-16'

--  with CTE_GrowthRates as
--  (
--  select date, county, state, fips, Cases, Casesper1000, CaseDensity1weekago, CasesOneWeekAgo, weeklyCaseDensityGrowthRate, Casesper1000 - CaseDensity1weekAgo as CaseDensityChange,
--         ROW_NUMBER() OVER (partition by fips ORDER BY Date ASC) RowNumber,
--		 AVG(WeeklyCaseDensityGrowthRate) OVER (partition by fips ORDER BY date ASC ROWS 6 PRECEDING) AS GrowthRate7DayAvg
-- from #CountyStats
-- where WeeklyCaseDensityGrowthRate is not null
-- AND fips is not null
-- )
-- select a.date, a.county, a.state, a.fips, a.cases, casesoneweekago, CasesPer1000, CaseDensity1weekago, weeklyCaseDensityGrowthRate,CaseDensityChange,
-- CAST(ROUND(GrowthRate7DayAvg, 3) as numeric(7,3)) as GrowthRate7DayAvg
-- ,  CAST(ROUND(CaseDensityChange, 3) as numeric(7,3)) as CaseDensityChange7DayAvg
-- , b.shape
-- into CountyLevelCaseGrowthRateChange_0716
-- from CTE_GrowthRates a inner join dbo.usCounties b
-- on a.fips = b.fips
-- order by 2, 1

-- select min(date) from CountyLevelCaseGrowthRateChange_0716

-- drop table CountyLevelCaseGrowthRateChange_0716



-- ---  Flourish datatable

-- select state, date, StateCaseWeeklyGrowthRate
-- into #StateCasesGrowth
--from #StateAllNumbers
--where date >= CONVERT(date, getdate() - 1)
--order by state, date

--select * 
--from (
--select state, statecaseweeklygrowthRate as WeeklyCaseGrowth
--from #StateCasesGrowth) as sourceTable
--pivot
--(
--MAX(StatecaseweeklyGrowthRate)
--for Date between '03-01-2020' and CONVERT(date, getdate() - 1)
--) piv



--drop table #tempDates

--DECLARE @cols AS NVARCHAR(MAX),
--    @query  AS NVARCHAR(MAX)

--;with cte (datelist, maxdate) as
--(
--    select min(date) datelist, max(date) maxdate
--    from #statecasesgrowth
--    union all
--    select date, maxdate
--    from cte
--    where datelist < maxdate
--) 
--select c.datelist
--into #tempDates
--from cte c


--select @cols = STUFF((SELECT distinct ',' + QUOTENAME(convert(CHAR(10), datelist, 120)) 
--                    from #tempDates
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,1,'')

