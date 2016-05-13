input <- CARICO UN FILE INPUT DA TEMINALE
output <- CARICO UN FILE INPUT DA TEMINALE
temp <- CARICO UN FILE INPUT DA TEMINALE

conf <- CONFIGURATION

job1 <- INIZIALIZZO IL PRIMO JOB

	SETTO COME INPUT DEL job1 IL FILE input
	SETTO COME OUTPUT DEL job1 IL FILE temp

	MAPPER1
		line <- PRENDO riga DA FILE input
		IF line HA "," IN POSIZIONE 8
			date <- PRENDO I PRIMI 6 VALORI				(2015-1)
			stringProd <- PRENDO DAL CARATTERE 9
		IF line HA "," IN POSIZIONE 9
			IF line HA "-" IN POSIZIONE 7
				date <- PRENDO I PRIMI 7 VALORI			(2015-10)
			IF line HA "-" IN POSIZIONE 6
				date <- PRENDO I PRIMI 6 VALORI			(2015-1)
			stringProd <- PRENDO DAL CARATTERE 10
		IF line HA "," IN POSIZIONE 10
			date <- PRENDO I PRIMI 7 VALORI			(2015-10)
			sringProd <- PRENDO DAL CARATTERE 11
		arrayProd[] <- SPLITTO stringProd A ","
		FOR  prodotto IN stringProd
			all <- date + " " + prodotto
			IMPOSTO IN conext KEY=all E VALUE=1

	COMBINER1
		PRENDO key E values PASSATI DAL MAPPER
		FOR val IN values
			sum <- SOMMO TUTTI GLI ELEMENTI DI values
		IMPOSTO IN context KEY=key E VALUE=sum

	REDUCER1
		PRENDO key E values DAL COMBINER
		new_item[] <- SPLITTO key A " "
    	FOR val IN values
    		v <- val
    		new_value <- new_item[1] + " " + v
    		new_date <- new_item[0]						
    	IMPOSTO IN context KEY=new_date E VALUE=new_value

FINE DI job1 CHE SCRIVE I RISULTATI IN temp

job2 <- INIZIALIZZO IL SECONDO JOB

    SETTO COME INPUT DEL job2 IL FILE temp
	SETTO COME OUTPUT DEL job2 IL FILE output

	MAPPER2
		line<-PRENDO riga DA FILE input
		IF line HA "\t" IN POSIZIONE 6
			date <- PRENDO I PRIMI 6 CARATTERI DI line	(2015-1)
			prod <- PRENDO DAL SETTIMO CARATTERE DI line

		IF line HA "\t" IN POSIZIONE 7
			date <- PRENDO I PRIMI 7 CARATTERI   (2015-10)
			prod <- PRENDO DAL OTTAVO CARATTERE
		IMPOSTO IN context KEY=date E VALUE=prod

	REDUCER2
		PRENDO key E values DAL MAPPER
		FOR val IN values
			all[] <- SPLITTO val IN " "
			n <- all[1]
			p <- all[0]
			countProd.put(p, n)         (countProd Ë UNA MAPPA)

		sortProd <- sortByValues(countProd) (sortProd Ë UNA MAPPA ORDINATA IN MANIERA DESCRESCENTE PER VALORI CON IL METODO sortByValues)

		result <- CI METTO I PRIMI 5 ELEMENTI DI sortProd

		IMPOSTO IN context KEY=key E VALUE=result



FINE DI job2 CHE SCRIVE I RISULTATI IN temp

