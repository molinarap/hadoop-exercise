Creo la tabella da cui attingo i vari job. contiene un campo DATE e un array con i prodotti degli scontrini

CREATE TABLE scontrini (dat DATE, products ARRAY<STRING>)

job1:
creo una tabella

CREATE TABLE productDate AS 
	SELECT DATE_FORMAT(dat, "yyyy-MM") AS month, product, count(1)
	AS count
	FROM scontrini LATERAL VIEW EXPLODE(products) TEMPORARY AS product
	ORDER BY month, product
	ORDER BY count desc

raggruppo per mese e calcolo i primi 5

CREATE TABLE result AS 
	SELECT month, COLLECTsET(CONCAT(product, ",", COUNT))
	FROM productDate
	GROUP BY month