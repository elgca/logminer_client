# logminer_client

oracle logminer client.
get CDC data from oracle.

# create a oracle 11g

```sh
docker pull iatebes/oracle_11g
docker run -d -p 1521:1521 --name=oracle_11g iatebes/oracle_11g
# 数据库必须处于archivelog模式，并且必须启用补充日志记录。
docker exec -it oracle_11g /bin/bash
su - oracle
sqlplus / as sysdba
SQL>shutdown immediate
SQL>startup mount
SQL>alter database archivelog;
SQL>alter database open;
# 启用补充日志记录
sqlplus / as sysdba
SQL>alter database add supplemental log data (all) columns;
create role logmnr_role;
grant create session to logmnr_role;
grant  execute_catalog_role,select any transaction ,select any dictionary to logmnr_role;
create user logmnr identified by logmnr;
grant  logmnr_role to logmnr;
alter user logmnr quota unlimited on users;

# 创建数据用户
create user producer identified by producer;
grant connect, resource to producer;

```

创建数据库表

```sql
create table EMP
(
EMPNO NUMBER(4) PRIMARY KEY,
ENAME VARCHAR2(10),
JOB VARCHAR2(9),
MGR NUMBER(4),
HIREDATE DATE,
SAL NUMBER(7,2),
COMM NUMBER(7,2),
DEPNO NUMBER(4)
);
```

```config
jdbc.hostname=localhost
jdbc.port=1521
jdbc.database=orcl
jdbc.user=logmnr
jdbc.password=logmnr
logmnr.tables=PRODUCER.EMP
name=test_001
dictionary.mode=DICT_FROM_ONLINE_CATALOG
fetch.size=1
local.cache.path=tmp
```

```sql
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
```

