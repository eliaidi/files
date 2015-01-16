/*SELECT *
FROM SHP_W01.ADDRESS_MIGRATION
WHERE STATUS = 'active';

SELECT COUNT(*)
FROM SHP_W01.ADDRESS_MIGRATION
WHERE STATUS = 'active';

SELECT COUNT(*)
FROM SHP_W01.ADDRESS_ATTRIBUTE;

SELECT *
FROM SHP_W01.ADDRESS_ATTRIBUTE;


SELECT COUNT(*) FROM (
SELECT ADDRESS_ID
FROM SHP_W01.ADDRESS_ATTRIBUTE
GROUP BY ADDRESS_ID
);


SELECT AA.*
FROM SHP_W01.ADDRESS_MIGRATION AM INNER JOIN SHP_W01.ADDRESS_ATTRIBUTE AA ON (AM.ID = AA.ADDRESS_ID)
WHERE AM.STATUS = 'active' AND AM.STATE_ID = 'BR-DF' AND CITY_ID IS NOT NULL;
*/


DECLARE
  FROM_DATE DATE;
  TO_DATE DATE;
  PROCESS_DATE DATE;
  MIGRADAS NUMBER;
  TOTAL NUMBER;
BEGIN
  
  FOR I IN 1..3
  LOOP
    PROCESS_DATE:= SYSDATE - I;
    
    FROM_DATE:= TO_CHAR(PROCESS_DATE - 1,'DD-Mon-YY');
    TO_DATE:= TO_CHAR(PROCESS_DATE,'DD-Mon-YY');
    
    DBMS_OUTPUT.put_line('FROM_DATE=' || FROM_DATE);
    SYS.DBMS_OUTPUT.put_line('TO_DATE=' || TO_DATE);
    
    SELECT COUNT(*) INTO TOTAL
    FROM SHP_W01.ADDRESS_MIGRATION AM
    WHERE AM.DATE_CREATED BETWEEN FROM_DATE AND TO_DATE;
    
    SELECT COUNT(*) INTO MIGRADAS
    FROM (SELECT AA.ADDRESS_ID
      FROM SHP_W01.ADDRESS_MIGRATION AM, SHP_W01.ADDRESS_ATTRIBUTE AA
      WHERE AM.DATE_CREATED BETWEEN FROM_DATE AND TO_DATE AND AM.ID = AA.ADDRESS_ID
      GROUP BY AA.ADDRESS_ID
    );
    
    SYS.DBMS_OUTPUT.put_line('TOTAL=' || TOTAL);
    SYS.DBMS_OUTPUT.put_line('MIGRADAS=' || MIGRADAS);
    SYS.DBMS_OUTPUT.put_line('MIGRADAS(%)=' || (MIGRADAS*100)/TOTAL);
        
  END LOOP;
  
END;

/*  
SELECT COUNT(*)
FROM SHP_W01.ADDRESS_MIGRATION AM
WHERE AM.DATE_CREATED between '21-Mar-13' AND '22-Mar-13';


SELECT COUNT(*) FROM (
SELECT AA.ADDRESS_ID
FROM SHP_W01.ADDRESS_MIGRATION AM, SHP_W01.ADDRESS_ATTRIBUTE AA
WHERE AM.DATE_CREATED between '21-Mar-13' AND '22-Mar-13' AND AM.ID = AA.ADDRESS_ID
GROUP BY AA.ADDRESS_ID
);
*/
/*
SELECT *
FROM ALL_IND_COLUMNS
WHERE INDEX_OWNER = 'SHP_W01' AND TABLE_NAME = 'ADDRESS_MIGRATION';
*/

