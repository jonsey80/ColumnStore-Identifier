drop table if exists #tables
drop table if exists #compress
create table #tables (
[databaseName] Nvarchar(100) null,
[index_id] int null,
[schemaName] Nvarchar(100) null,
[tableName] Nvarchar(200) Null,
[Rows] bigint null,
[partitions] Nvarchar(10) null,
[totalspaceMB] numeric(36,2) null,
[Compresses] numeric(36,2) null,
[read/write ratio (est)] numeric(18,2) null
)

create table #compress (
objectname Nvarchar(100),
schemaname Nvarchar(100),
indexid int,
partitionnumber int,
size numeric(36,2),
compr numeric(36,2),
samplesize numeric(36,2),
samplecompr numeric(36,2)
)

declare @sql Nvarchar(4000),
	 @index Nvarchar(10),
	@db Nvarchar(100),
	@schema Nvarchar(100),
	@table Nvarchar(100)

exec sp_msforeachdb ' use [?]

insert into #tables([databaseName],[index_id],[schemaName],[tableName],[Rows],[partitions],[totalspaceMB],[read/write ratio (est)])
select distinct DB_name() ''Database'',i.index_id,  s.name , t.name, sum(p.rows)''rows'',count(distinct partition_id)''partitions'', 
CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,rat.[Read/write ratio] FROM	sys.schemas AS S 
INNER JOIN sys.tables AS T ON	(S.schema_id = T.schema_id) 
inner join sys.partitions as P on t.object_id = p.object_id
INNER JOIN sys.indexes AS I ON	(T.object_id = I.object_id)
inner join sys.allocation_units a ON p.partition_id = a.container_id
inner join (select object_id from sys.tables 
where object_id not in (
select tabl.object_id from sys.tables tabl
inner join sys.columns col on tabl.object_id = col.object_id
inner join sys.types typ on col.user_type_id = typ.user_type_id
where col.max_length = -1 )) dat on T.object_id = dat.object_id
inner join ( SELECT	DDIUS.object_id, DDIUS.index_id,
		
		(
			(cast(DDIUS.user_seeks as numeric(18,2)) +
			DDIUS.user_scans +
			DDIUS.user_lookups) /
			case when DDIUS.user_updates = 0 then 1 else DDIUS.user_updates end 
		)	*100 ''Read/write ratio''									
		
FROM sys.dm_db_index_usage_stats AS DDIUS 
	where database_id = db_id()
			
	) rat on t.object_id = rat.object_id  and I.index_id = rat.index_id 
where i.type not in (2,3,4,5,6) and DB_Name() not in (''next_DSS'',''tempdb'')  
and rat.[Read/write ratio] < 10 
group by i.index_id,s.name,t.name ,rat.[Read/write ratio]
having sum(p.rows) > 10000000 and count(distinct partition_id) = 1 and db_id() > 4 


 '



declare my_cursor cursor for (
select databaseName,index_id,schemaName,tableName from #tables
)

open my_cursor 

fetch next from my_cursor into @db,@index,@schema,@table 

while @@FETCH_STATUS = 0
begin
print @db
set @sql = N'use [' + @db +']

insert into #compress
exec sp_estimate_data_compression_savings @index_id =' + @index +', @schema_name =' + @schema + ', @object_name =' + @table+', @data_compression = ''COLUMNSTORE'',@partition_number = 1 '

exec sp_executesql @sql

set @sql = N'
update #tables 
set Compresses = (select  size - compr from #compress)
where [databaseName] = ''' +@db+''' and schemaName ='''+ @schema +''' and tableName = ''' + @table +''''

exec sp_executesql @sql

truncate table #compress


fetch next from my_cursor into @db,@index,@schema,@table  
end 
close my_cursor
deallocate my_cursor

select databaseName, schemaName + '.' + tableName, rows, partitions, totalspaceMB, [Compresses]/1024 'Est. space saved (MB)' from #tables

