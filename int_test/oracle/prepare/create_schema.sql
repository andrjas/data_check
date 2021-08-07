declare
    object_exists integer;
begin
    select count(*) into object_exists from dba_users where username='MAIN';
    if (object_exists = 1) then
        execute immediate 'DROP USER MAIN CASCADE';
    end if;
    execute immediate 'CREATE USER MAIN NO AUTHENTICATION DEFAULT TABLESPACE SYSTEM';
    execute immediate 'GRANT RESOURCE TO MAIN';
    execute immediate 'GRANT UNLIMITED TABLESPACE TO MAIN';
    begin
        execute immediate 'DROP TABLE CT_TEST';
    exception when others then null;
    end;
end;
