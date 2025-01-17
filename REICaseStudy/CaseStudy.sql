
-- Import shapefile of zip demographics 
-- export the results to sql server through a database connection
-- run sql query to rename, sum, and rank some of the fields
-- Select the results into a new table 

drop table localgisdata.dbo.ZipCodeProcessed


select UniqueID
, [Key_] as zipcode
, CYA01V001 as TotalPop
, CYB02V001 as TotalHouseholds
, CYPOPDENS as PopDensity
, PYCYPOPGRW as PopulationGrowth
, CYEC17V001 as MedianHHInc
, XCYA04V005 + XCYA04V006 + XCYA04V007 + XCYA04V008 + XCYA04V009 + XCYA04V010 + XCYA04V011 + XCYA04V012 as PercentofPop18_44
, CYA16VV01 as MeanAge
, ATOTALEXP as AvgConExp2016
, TCTOTALEXP as TotConExp2016
, FATOTALEXP as AvgConExpProj
, TFTOTALEXP as TotConExpProj
, RANK() over (order by ATOTALEXP desc) as RankedAvgConExp
, shape
into localgisdata.REICaseStudy.ZipCodeProcessed
 FROM [LocalGISData].[dbo].[ZIPCODERAW]

select * from REICaseStudy.zipcodeprocessed  -- 30573 records



--NEIGHBORHOOD ANALYSIS

-- Consider the zipcodes that touch each other and aggregate some of the columns to look at more than just 
-- the individual zipcodes. This should help identify demand in an area rather than just individual zips
drop table neighbortable

-- create the table neighbortable to identify the zipcodes that touch each zipcode in the map
SELECT a.Key_ as BaseZip
	, b.Key_ as NeighborZip
INTO REICaseSTudy.NeighborTable
FROM localgisdata.dbo.ZIPCODERAW a 
		cross join localgisdata.dbo.ZipCodeRaw b
WHERE a.shape.STTouches(B.shape) = 1
--and a.Key_ <> b.Key_
order by 1


-- aggregates for neighboring zip codes
-- This table contains aggregates for the neighboring zipcodes for each zip in the analysis.
-- While the zip analysis is great it's best to consider both individual zips and zips that touch each other
-- This query was used to create aggregates based on the Neighbor analysis done in the early step

select b.basezip as TargetZipCode
	, COUNT(zipcode) as totalneighbors
	, SUM(TotalHouseholds) as TotalNeighborHH
	, SUM(Totalpop) AS TotalPopNeighbors
	, AVG(PopulationGrowth) as NeighborPopGrowth
	, AVG(MedianHHInc) as MedianIncomeNeighbors
	, AVG(RankedAvgConExp) as AverageRankConSpendingNeighbors
	, AVG(meanAge) as AverageAgeNeighbors
	, sum(PercentofPop18_44 * totalpop)/IIF(SUM(totalpop) = 0,NULL,SUM(TotalPop)) as NeighborPercentofPOP18_44
	, AVG(AvgConExpProj) as AverageProjectedConSpendNeighbor
INTO REICaseStudy.NeighborAggregates
	from LocalGISData.REICaseStudy.ZipCodeProcessed a left outer join REICaseStudy.NeighborTable b
on a.zipcode = b.neighborzip
group by b.basezip
order by 1

-- open the zipcodeprocessed table in arcgis (through the database connection) and 
-- run the grouping analysis tool on it.  Use all the attributes and the results will
-- cluster the zips into groups with like variables.

-- move the output from Grouping tool into the proper database schema for the project
alter schema REICaseStudy transfer dbo.zipgroups

-- Get counts for each zip group that highest performing stores fall 
--  into (? 20,000,000 revenue in 2016)

select SS_group, COUNT(*) as TotalStoresinGroup
from dbo.REIStoreLocations a inner join REICaseStudy.ZipGroups b
on A.shape.STWithin(B.shape) = 1
where A.YearlyRevenue2016 > 20000000
group by ss_group
order by 2 desc

-- groups 4 contains 33 of the 52 highest performing stores
-- Group 4 based on the demand attributes are the best potential zipcodes for new store locations

------------------------------------COMPETITION -------------------------------------------------------------------------------

-- WHAT ABOUT COMPETITORS
-- Bring the competitors information into ArcGIS and export to SQL Server with geometry 
-- information.

-- identify competitors and assign a weight based on SIC code for how direct a competitor
-- the locations are.  3 = Direct all products, 2 = Only compete in certain products, 1 = minor competitor
-- Set a field competitortype

select distinct sic_code from LocalGISData.dbo.REICompetitors
where Competitortype is null

alter table REICompetitors add CompetitorType integer 

update REICompetitors 
set Competitortype = 3 
where sic_code like ('%5941%') or SIC_CODE like ('%3949%')

update REICompetitors
Set competitortype = 2 
where sic_code like ('%2329%') or sic_code like ('%2399%') or sic_code like ('%5651%')
or sic_code like ('%3751%') or sic_code like ('%3799%')

update REICompetitors
Set Competitortype = 1
where competitortype is null

-- Get a ZipCompetitionLevel by summing all competitorytypes for each zip code
drop table REICaseStudy.Competition

select A.zipcode
	, A.shape -- make it mappable
	, x.zipCompetitionLevel
into REICaseStudy.Competition
from REICaseStudy.zipcodeprocessed a inner join 
(
		select A.zipcode
			, COUNT(objectID) as TotalCompetitors
			, isNull(SUM(B.competitortype),0) as ZipCompetitionLevel
		from REICaseStudy.zipcodeprocessed a left outer join REICompetitors b
		on A.shape.STContains(B.shape) = 1
		group by A.zipcode 
) x
on x.zipcode = a.zipcode

-- Consider the neighborhoods as well.  What is the ZipCompetitionLevel for the sum of all 
select B.BaseZip as Zipcode
	, SUM(zipcompetitionLevel) as NeighborCompetion
INTO REICaseStudy.NeighborhoodCompetition
from REICaseStudy.Competition a left outer join REICaseStudy.neighbortable b
on a.zipcode = b.neighborzip
group by B.basezip

---------------------------------------------Potential Space Available ----------------------------------------------------------------

--- Shopping Centers
-- import the shopping centers data into ArcGIS and then export to sql server with geometry info

select * from REICaseStudy.shoppingcenters

-- Identify the total number of shopping centers per zipcode and sum up the Gross Leasing Area (GLA) 
-- The gross leasing Area information needed to be reformatted and coverted to integer.

select a.zipcode
	, COUNT(objectid) as NumberofShoppingCenters
	, SUM(CAST(LEFT(REPLACE(isNULL(GLA,0), ',', ''), LEN(replace(GLA, ',', '')) - 5) as int)) as ZipCodeShoppingCenterGLA
INTO REICaseStudy.AvailableSpace
from REICaseStudy.zipcodeprocessed a left outer join REICaseStudy.shoppingCenters b
on A.shape.STContains(B.shape) = 1
group by A.zipcode
order by 2 desc

--- Put it all together for what-if scenarios and visualization

--drop table REICaseStudy.FinalZipAnalysis

select A.zipcode
	, A.shape
	, E.SS_Group as DemandGroup
	, A.TotalPop
	, A.TotalHouseholds
	, A.PopulationGrowth
	, A.MedianHHInc 
	, A.PercentofPop18_44
	, A.MeanAge
	, A.RankedAvgConExp
	, A.TotConExp2016
	, B.TotalNeighbors
	, B.TotalNeighborHH
	, B.TotalPopNeighbors
	, B.NeighborPopGrowth
	, B.AverageRankConSpendingNeighbors
	, C.zipcompetitionLevel
	, g.NeighborCompetion as ZipNeighborCompetitionLevel
	, d.NumberofShoppingCenters
	, d.ZipCodeShoppingCenterGLA
	, f.REI_Store_ID
INTO REICaseStudy.FinalZipAnalysis
from REICaseStudy.ZipCodeProcessed a left outer join REICaseStudy.NeighborAggregates b
on A.zipcode = b.targetZipcode
Left Outer join REICaseStudy.Competition c
on A.zipcode = c.zipcode 
Left outer join REICaseStudy.AvailableSpace d
on A.zipcode = d.zipcode 
left outer join REICaseStudy.ZipGroups e
on A.zipcode = E.zipcode
left outer join dbo.REIStoreLocations f
on A.zipcode = f.zip_code
left outer join REICaseStudy.NeighborhoodCompetition g
on a.zipcode = g.zipcode


-- Queries can be done either in SQL Server or the table can be visuallized in ArcGIS software and built that way
-- Here's an example of a new store locator query.   

select *
from REICaseStudy.FinalZipAnalysis
where DemandGroup = 4 -- Based on the demographic variables and over performing store locations
and REI_Store_ID is NULL -- no existing store in the zipcode
and zipcompetitionLevel < 10 -- minimal direct competition
and ZipNeighborCompetitionLevel < 35  -- how much competition exists in the neighborhood
and NumberofShoppingCenters > 0 -- is there an existing location for a new store

-- Based on the above variables 214 zip based sites are potential sites for new stores.
