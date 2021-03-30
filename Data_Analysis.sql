/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [INCIDENT_NUMBER]
      ,[OFFENSE_CODE]
      ,[OFFENSE_CODE_GROUP]
      ,[OFFENSE_DESCRIPTION]
      ,[DISTRICT]
      ,[REPORTING_AREA]
      ,[SHOOTING]
      ,[OCCURRED_ON_DATE]
      ,[YEAR]
      ,[MONTH]
      ,[DAY_OF_WEEK]
      ,[HOUR]
      ,[UCR_PART]
      ,[STREET]
      ,[Lat]
      ,[Long]
  FROM [Boston_Crime].[dbo].[Crimes]



select count(*) from Crimes --6578374

select OFFENSE_CODE, OFFENSE_CODE_GROUP, count(*) Incidents from Crimes group by OFFENSE_CODE, OFFENSE_CODE_GROUP order by count(*) desc

select OFFENSE_CODE_GROUP, count(*) Incidents from Crimes group by OFFENSE_CODE_GROUP order by count(*) desc

select OFFENSE_CODE, OFFENSE, count(*) Incidents from Crimes group by OFFENSE_CODE, OFFENSE order by count(*) desc

select OFFENSE_CODE, count(*) Incidents from Crimes group by OFFENSE_CODE order by count(*) desc

select distinct shooting from crimes

select offense_code, offense, count(*) from Crimes group by offense_code, OFFENSE order by count(*) desc

select distinct offense_code, offense from 
(select offense_code, offense, count(*) counts from (select * from Crimes where OFFENSE like '%ASSAULT%' or OFFENSE like '%LARCENY%' or OFFENSE like '%BURGLARY%' or OFFENSE like '%FRAUD%')a group by offense_code, OFFENSE) x order by x.OFFENSE_CODE

select count(*) from Crimes where OFFENSE like '%ASSAULT%'

--Fraud
select count(*) from Crimes where OFFENSE_CODE in ('1102','1105','1106','1107','1108','1109')

--Assault
select count(*) from Crimes where OFFENSE_CODE in ('402','403','413','423','432','801','802')

--Burglary
select count(*) from Crimes where OFFENSE_CODE in ('520','521','522','540','541','542','560','561','562')

--Larceny
select count(*) from Crimes where OFFENSE_CODE in ('611','612','613','614','615','616','617','618','619','623','624','627','629','633','634','637','639')


select count(*) from Crimes where OFFENSE like '%FRAUD%'
select count(*) from Crimes where OFFENSE like '%LARCENY%'
select count(*) from Crimes where OFFENSE like '%BURGLARY%'



select top 10 * from Crimes order by OCCURRED_ON_DATE desc