input <- CARICO UN FILE INPUT DA TEMINALE
output <- CARICO UN FILE INPUT DA TEMINALE
temp <- CARICO UN FILE INPUT DA TEMINALE

conf <- CONFIGURATION

job1 <- INIZIALIZZO IL PRIMO JOB

	SETTO COME INPUT DEL job1 IL FILE input
	SETTO COME OUTPUT DEL job1 IL FILE temp

	MAPPER1
		line <- PRENDO riga DA FILE input

		IF line HA "," NELLA POSIZIONE 8
			date <- PRENDO I PRIMI 5 VALORI  //EX. yyyy-m-d,
			stringProd <- PRENDO I PRIMI 9 VALORI

		IF line HA "," NELLA POSIZIONE 9
			IF date HA "-"  alla posizione 7
				date <- PRENDO I PRIMI 7 VALORI  //EX. yyyy-mm-d,
			IF date HA "-"  alla posizione 6
				date <- PRENDO I PRIMI 6 VALORI  //EX. yyyy-m-dd,
			stringProd <- PRENDO I PRIMI 11 VALORI

		IF line HA "," NELLA POSIZIONE 10
			date <- PRENDO I PRIMI 6 VALORI  //EX. yyyy-mm-d,
			stringProd <- PRENDO I PRIMI 11 VALORI

		IF stringProd != NULL
			arrayProd <- SPLITTO A "-" stringProd
			FOR i DI arrayProd
				all <- date + " " + arrayProd[i]
				IMPOSTO in context KEY=all E VALUE=1 PER PASSARLO AL COMBINER1

	COMBINER1
		sum <- INIZIALIZZO A 0

		PRENDO key CHE MI VIENE PASSATO DAL MAPPER
		PRENDO values CHE MI VIENE PASSATO DAL MAPPER

		FOR i IN values 
			SUM = SUM + (PRENDO VALORE DI values[i])
		
		IMPOSTO in context KEY=key E VALUE=SUM PER PASSARLO AL REDUCER1

	REDUCER1
		PRENDO key CHE MI VIENE PASSATO DAL COMBINER
		PRENDO values CHE MI VIENE PASSATO DAL COMBINER

		FOR v IN values
			v <- PRENDO VALORE DI values

		new_item <- SPLITTO A " " key	//EX. 2015-1	acqua/3
		new_date <- new_item[0]	//EX. 2015-1
		new_value <- new_item[1]	//EX. acqua/3 

		namePrice <- SPLITTO A "/" key	//EX. acqua/3 
		nameProd <- namePrice[0]	//EX. acqua
		priceProd <- namePrice[1]	//EX. 3 

		totalPriceProd <- priceProd * v

		datePrice <- new_date + ":" + totalPriceProd
		
		IMPOSTO in context KEY=nameProd E VALUE=datePrice PER SCRIVERLO IN output

FINE DI job1 CHE SCRIVE I RISULTATI IN temp

job2 <- INIZIALIZZO IL PRIMO JOB

	SETTO COME INPUT DEL job2 IL FILE temp
	SETTO COME OUTPUT DEL job2 IL FILE output

	MAPPER2
		line <- PRENDO riga DA FILE temp

		prodAll <- SPLITTO A "\t" key	
		prodName <- prodAll[0]
		prodDatePice <- prodAll[1]
		
		IMPOSTO in context KEY=prodName E VALUE=prodDatePice PER PASSARLO AL REDUCERD2

	REDUCER2
		PRENDO key CHE MI VIENE PASSATO DAL COMBINER
		PRENDO values CHE MI VIENE PASSATO DAL COMBINER

		countProd <- INIZIALIZZO UNA MAP<key, value>
		
		FOR v IN values
			v <- PRENDO VALORE DI values
			all <- SPLITTO A ":" v
			p = all[0]
			dp = all[1]
			IMPOSTO in countProd KEY=p E VALUE=dp 

		FOR i IN countProd.key
			result = result + ":" + countProd.get(i) + " "

		IMPOSTO in context KEY=key E VALUE=result PER SCRIVERLO IN output


FINE DI job2 CHE SCRIVE I RISULTATI IN temp

