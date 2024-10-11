USE reqleg;
CREATE TABLE CUSTOMERS_FINAL AS
SELECT
    cus.CUSTOMER_ID,
    NOM,
    PRENOM,
    SOCIALSECNO AS NO_CNI,
    PASSPORTNO,
    con.DNNUM AS MSISDN
FROM customers as cus
LEFT JOIN
(SELECT
    CUSTOMER_ID,
    DNNUM,
    MAX(CO_MODDATE) AS MODDATE
FROM
    contracts
WHERE
    COSTAT = 'a'
    AND DATE_FORMAT(CO_MODDATE, '%Y-%m-%d') <> '0000-00-00'
    AND DATE_FORMAT(CO_ACTIVATED, '%Y-%m-%d') <> '0000-00-00'
	AND DATE_FORMAT(CO_MODDATE, '%Y-%m-%d') <= (SELECT DATE_FORMAT(DATEHEURE, '%Y-%m-%d') FROM transactions_telco LIMIT 1)
GROUP BY
    CUSTOMER_ID,
    DNNUM) AS con
ON cus.CUSTOMER_ID = con.CUSTOMER_ID;