SELECT *
FROM SHP_W01.SHIPMENT
WHERE DATE_CREATED LIKE '07-MAR-13';


SELECT ID, SENDER_ID, TO_CHAR(DATE_FIRST_PRINTED,'DD-MON-YYYY HH:MI:SS')
FROM SHP_W01.SHIPMENT
WHERE ID IN (20669988482,20670123107,20669984828,20669821425,20669920652,20669920628,20669821266,20669996667,20670128171,20669996720);

20669984828,20669988482

UPDATE SHP_W01.SHIPMENT
SET DATE_FIRST_PRINTED = NULL
WHERE SENDER_ID = 8408542 AND ID IN (20669988482,20670123107,20669984828,20669821425,20669920652,20669920628,20669821266);


20669988482,20670123107,20669984828,20669821425