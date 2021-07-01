declare
    object_exists integer;
begin
    select count(*) into object_exists from dba_users where username='TEMP';
    if (object_exists = 1) then
        execute immediate 'DROP USER TEMP CASCADE';
    end if;
    execute immediate 'CREATE USER TEMP NO AUTHENTICATION DEFAULT TABLESPACE SYSTEM';
    execute immediate 'GRANT RESOURCE TO TEMP';
    execute immediate 'GRANT UNLIMITED TABLESPACE TO TEMP';
end;
