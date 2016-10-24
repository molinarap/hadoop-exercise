conf <- CONFIGURATION

job1 <- INIZIALIZZO IL PRIMO JOB

	SETTO COME INPUT DEL job1 IL FILE input
	SETTO COME OUTPUT DEL job1 IL FILE output

	MAPPER1
		line <- PRENDO riga DA FILE input
		arrayProd[] <- SPLITTO line IN ","
		mapElem <- countElemLines()      (mapElem Ë UNA MAPPA CHE PER CHIAVE HA IL PRODOTTO E PER VALORE IL NR DI VOLTE CHE IL PRODOTTO COMPARE NEL FILE)

		FOR i IN arrayProd
			FOR j IN arrayProd
				IF i != j
					q <- mapElem.get(arrayProd[i]) (q Ë IL NR DI VOLTE CHE IL PRODOTTO i COMPARE NEL FILE)
					couple <- arrayProd[i]+","+arrayProd[j]+":"+q  (couple Ë LA COPPIA PRODOTTO i, PRODOTTO j CON IL NR DI VOLTE CHE COMPARE i - EX (acqua, formaggio:154))
					l.add(couple)           (l Ë UNA LISTA DI COPPIE)

		c <- countLines()     (IL METODO countLines() CONTA LE LINEE DEL FILE)

		for (x IN l)
			coupleLines <- l[x] + '\t' + c;     (IN coupleLines METTO LA COPPIA E IL NUMERO DI RIGHE SEPARATE DA UN TAB)
			
			IMPOSTO IN context KEY=coupleLines E VALUE=1

	COMBINER1
		PRENDO key E values PASSATI DAL MAPPER
		FOR val IN values
			sum <- SOMMO TUTTI GLI ELEMENTI DI values
		IMPOSTO IN context KEY=key E VALUE=sum

	REDUCER1
		PRENDO key E values DAL COMBINER
		k[] <- SPLITTO key IN "\t"
		coupleNumber[] <- SPLITTO k[0] IN ":"
		count <- k[1]

		couple <- coupleNumber[0]
		n <- couple = coupleNumber[1]
		FOR val IN values
			v <- SOMMO TUTTI GLI ELEMENTI DI values

		sum <- (v/count)*100
		sum2 <- (v/n)*100

		IMPOSTO IN conext KEY=couple E VALUE=sum+"%, "+sum2+"%" PER SCRIVERLO IN output


FINE DI job1 CHE SCRIVE I RISULTATI IN output
