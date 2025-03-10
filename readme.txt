TODO:
Finish importer
    - table import - done
    - view import - done
    - sp import - done
    - function import - done
    - udt / udtt import - done
    - dependency import
    - keys and index import
    - reference import

Finish exporter
    - export drop statements (in order)
    - export create types (in order)
    - export create sps (in order)
    - export create tables (in order)
    - export create keys and indexes (in order)
    - export references (in order)

Finish Diff generator
    - generate diff model from existing model and existing schema (model)
    - export scripts for diff

Finish Script Executor
    - Execute scripts in order according to rules


SELECT * FROM INFORMATION_SCHEMA.SCHEMATA
select * from sys.schemas where name not in (select name from sys.database_principals)
select * from sys.database_principals

select s.name as schema_name, t.name as table_name from sys.tables t
inner join sys.schemas s
on t.schema_id = s.schema_id
where t.type = 'U'

select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT, ORDINAL_POSITION, COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY  from INFORMATION_SCHEMA.columns
order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION




where
TABLE_SCHEMA = '{db_name}' and TABLE_NAME='{table.name.raw()}' order by ORDINAL_POSITION

select * from information_schema.referential_constraints fks
join information_schema.key_column_usage kcu
on fks.constraint_schema = kcu.table_schema



select * from information_schema.referential_constraints
select * from information_schema.key_column_usage where CONSTRAINT_NAME = 'FK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID' order by CONSTRAINT_name, ORDINAL_POSITION
select * from information_schema.CONSTRAINT_COLUMN_USAGE where CONSTRAINT_NAME = 'FK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID'

select fks.constraint_name as constraint_name, fks.referenced_table_name as primary_table,
'FOREIGN KEY' as type from information_schema.referential_constraints fks
join information_schema.key_column_usage kcu
on fks.constraint_schema = kcu.table_schema
and fks.table_name = kcu.table_name
and fks.constraint_name = kcu.constraint_name
where fks.constraint_schema = '{db_name}' and fks.table_name = '{table.name.raw()}'
group by fks.constraint_name, fks.referenced_table_name

-- foreign keys
select fk.name as fk_constraint_name,
	schema_name(fk_tab.schema_id) + '.' + fk_tab.name as foreign_table,
    schema_name(pk_tab.schema_id) + '.' + pk_tab.name as primary_table,
    fk_cols.constraint_column_id as no,
    fk_col.name as fk_column_name,
    pk_col.name as pk_column_name
from sys.foreign_keys fk
    inner join sys.tables fk_tab
        on fk_tab.object_id = fk.parent_object_id
    inner join sys.tables pk_tab
        on pk_tab.object_id = fk.referenced_object_id
    inner join sys.foreign_key_columns fk_cols
        on fk_cols.constraint_object_id = fk.object_id
    inner join sys.columns fk_col
        on fk_col.column_id = fk_cols.parent_column_id
        and fk_col.object_id = fk_tab.object_id
    inner join sys.columns pk_col
        on pk_col.column_id = fk_cols.referenced_column_id
        and pk_col.object_id = pk_tab.object_id
order by schema_name(fk_tab.schema_id) + '.' + fk_tab.name,
    schema_name(pk_tab.schema_id) + '.' + pk_tab.name,
    fk_cols.constraint_column_id


--view and definition
select schema_name(v.schema_id) as schema_name,
       v.name as view_name,
       m.definition
from sys.views v
join sys.sql_modules m
     on m.object_id = v.object_id
 order by schema_name,
          view_name

--table and columns
select
	schema_name(tab.schema_id) as schema_name,
	tab.name as table_name,
    col.column_id as id,
    col.name,
    t.name as data_type,
    col.max_length,
    col.precision,
    col.is_nullable
from sys.tables as tab
    inner join sys.columns as col
        on tab.object_id = col.object_id
    left join sys.types as t
    on col.user_type_id = t.user_type_id
--where tab.name = 'Table name' -- enter table name here
-- and schema_name(tab.schema_id) = 'Schema name'
order by tab.name, column_id;

--primary keys
select schema_name(tab.schema_id) as [schema_name],
    pk.[name] as pk_name,
    substring(column_names, 1, len(column_names)-1) as [columns],
    tab.[name] as table_name
from sys.tables tab
    inner join sys.indexes pk
        on tab.object_id = pk.object_id
        and pk.is_primary_key = 1
   cross apply (select col.[name] + ', '
                    from sys.index_columns ic
                        inner join sys.columns col
                            on ic.object_id = col.object_id
                            and ic.column_id = col.column_id
                    where ic.object_id = tab.object_id
                        and ic.index_id = pk.index_id
                            order by col.column_id
                            for xml path ('') ) D (column_names)
order by schema_name(tab.schema_id),
    pk.[name]

-- indexes and unique keys
select schema_name(t.schema_id) + '.' + t.[name] as table_view,
    case when t.[type] = 'U' then 'Table'
        when t.[type] = 'V' then 'View'
        end as [object_type],
    case when c.[type] = 'PK' then 'Primary key'
        when c.[type] = 'UQ' then 'Unique constraint'
        when i.[type] = 1 then 'Unique clustered index'
        when i.type = 2 then 'Unique index'
        end as constraint_type,
    c.[name] as constraint_name,
    substring(column_names, 1, len(column_names)-1) as [columns],
    i.[name] as index_name,
    case when i.[type] = 1 then 'Clustered index'
        when i.type = 2 then 'Index'
        end as index_type
from sys.objects t
    left outer join sys.indexes i
        on t.object_id = i.object_id
    left outer join sys.key_constraints c
        on i.object_id = c.parent_object_id
        and i.index_id = c.unique_index_id
   cross apply (select col.[name] + ', '
                    from sys.index_columns ic
                        inner join sys.columns col
                            on ic.object_id = col.object_id
                            and ic.column_id = col.column_id
                    where ic.object_id = t.object_id
                        and ic.index_id = i.index_id
                            order by col.column_id
                            for xml path ('') ) D (column_names)
where is_unique = 1
and t.is_ms_shipped <> 1
order by schema_name(t.schema_id) + '.' + t.[name]

Select t.name   [TableTypeName]
      ,SCHEMA_NAME(t.schema_id)  [SchemaName]
      ,c.name   [Column Name]
      ,y.name   [Data Type]
      ,c.max_length
      ,c.precision
      ,c.is_identity
      ,c.is_nullable
From sys.table_types t
Inner join sys.columns c on c.object_id = t.type_table_object_id
Inner join sys.types y ON y.system_type_id = c.system_type_id
WHERE t.is_user_defined = 1
  AND t.is_table_type = 1


select object_name(object_id) as table_name,sc.name  from sys.columns sc
inner join sys.types st ON sc.user_type_id = st.user_type_id
where st.is_user_defined = 1


select * from sys.types t
where t.is_user_defined = 1

SELECT USER_NAME(TYPE.schema_id) + '.' + TYPE.name      AS "Type Name",
       COL.column_id,
       SUBSTRING(CAST(COL.column_id + 100 AS char(3)), 2, 2)  + ': ' + COL.name   AS "Column",
       ST.name                                          AS "Data Type",
       CASE COL.Is_Nullable
       WHEN 1 THEN ''
       ELSE        'NOT NULL'
       END                                              AS "Nullable",
       COL.max_length                                   AS "Length",
       COL.[precision]                                  AS "Precision",
       COL.scale                                        AS "Scale",
       ST.collation                                     AS "Collation"
FROM sys.table_types TYPE
JOIN sys.columns     COL
    ON TYPE.type_table_object_id = COL.object_id
JOIN sys.systypes AS ST
    ON ST.xtype = COL.system_type_id
where TYPE.is_user_defined = 1
ORDER BY "Type Name",
         COL.column_id

-- custom types
SELECT  UserType.[name] AS UserType
        , SystemType.[name] AS SystemType
        , UserType.[precision]
        , UserType.scale
        , UserType.max_length AS bytes
            --This value indicates max number of bytes as opposed to max length in characters
            -- NVARCHAR(10) would be 20 / VARCHAR(10) would be 10
        , UserType.is_nullable
  FROM  sys.types UserType
      JOIN sys.types SystemType
          ON SystemType.user_type_id = UserType.system_type_id
             AND SystemType.is_user_defined = 0
 WHERE  UserType.is_user_defined = 1
 ORDER BY UserType.[name]



 SELECT OBJECT_NAME(referencing_id) AS entity_name, SCHEMA_NAME(o.schema_id) as entity_schema, o.type as entity_type,
    referenced_entity_name, referenced_schema_name, ref.type as referenced_type
FROM sys.sql_expression_dependencies AS sed
INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
inner join sys.objects as ref on ref.object_id = referenced_id
where ref.type in ('P', 'FN')
and o.type in ('P', 'FN')


WHERE referencing_id = OBJECT_ID(N'dbo.test2');




go
create proc test1 @dt datetime out as
begin
	set @dt = GETDATE()
end
go

create proc test2 @dt datetime out as
begin
	exec test1 @dt
end
go

declare @d datetime
exec test2 @d
print @d

go
create function func1() returns datetime as
begin
	return getdate()
end

go
create function func2() returns datetime as
begin
	return dbo.func1()
end


print dbo.func2()


select st.name as table_name, SCHEMA_NAME(st.schema_id) as schema_name,  chk.definition, chk.name as constraint_name, chk.type
from sys.check_constraints chk
inner join sys.columns col
 on chk.parent_object_id = col.object_id
inner join sys.tables st
 on chk.parent_object_id = st.object_id
 order by st.name, col.column_id


SELECT  obj.name AS FK_NAME,
    sch.name AS [schema_name],
    tab1.name AS [table],
    col1.name AS [column],
    tab2.name AS [referenced_table],
    col2.name AS [referenced_column]
FROM sys.foreign_key_columns fkc
INNER JOIN sys.objects obj
    ON obj.object_id = fkc.constraint_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fkc.parent_object_id
INNER JOIN sys.schemas sch
    ON tab1.schema_id = sch.schema_id
INNER JOIN sys.columns col1
    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fkc.referenced_object_id
INNER JOIN sys.columns col2
    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
order by obj.name