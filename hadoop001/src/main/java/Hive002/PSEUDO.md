la tabella productDate serve a calcolare per ogni prodotto per ogni mese il nr di scontrini in cui compare
la tabella result si ottiene facendo un join tra la tabella productCount e la tabella prices per ottenere il ricavo di ogni prodotto per mese

oltre alla tabella scrontrini (come prima) creo una tabella con il prezzo di ogni prodotto

CREATE TABLE prices(product STRING, price DOUBLE)
...

creo la tabella productDate come nel job1
infine
CREATE TABLE risultati AS
	SELECT product, COLLECT SET(CONCAT(month,"",price*count))
	FROM productDate JOIN price
	WHERE productDate.product = prices.product
	GROUB BY product