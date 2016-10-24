
conf <- CREO UNA CONFIGARAZIONE SPARK
sc <- CREO UN CONTEXT SPARK

PRENDO IN INPUT UN FILE

CREO UNA RDD foods SPLITTANDO A "/n" PER LAVORARE SULLE RIGHE DEL FILE

mapElem <- INIZIALIZZO UNA MAP

SCORRO foods PER CREARE UN RDD dateFoods
	line <- foods.element
	arrayProd SPLITTO A "," line
	l <- INIZIALIZZO UNA LISTA
	mapElem <- NUMERO DI RIGHE NEL FILE INPUT TRAMITE METODO ESTERNO
	C <- NUMERO DI VOLTE CHE UN foods.element E' CONTENUTO NEL FILE

	FOR i IN arrayProd
		FOR j IN arrayProd
			SE i!=j
			 SE mapElem CONTIENE LA CHIAVE arrayProd[i]
			 	q <- PRENDI VALUE CON KEY arrayProd[i]
			 	couple <- arrayProd[i]+","+arrayProd[j]+":"+q+ '\t' + c
			 	AGGIUNGO couple A l
	RETURN l

RETURN dateFoods (UGUALE A l)

SCORRO dateFoods PER CREARE UN MAPPA(Tuple2) mapFoods
	KEY=dateFoods.key E VALUES=1

SCORRO mapFoods PER CREARE UN MAPPA(Tuple2) result
	KEY=mapFoods.key E VALUES= VALUES + mapFoods.value

RETURN result

SCORRO priceFoods PER CREARE UN MAPPA(Tuple2) mapFoods

	line <- priceFoods.element

	k SPLITTO A "\t" line
	countTemp SPLITTO A "," k[1]

	count <- countTemp[0] (LO PARSO A DOUBLE)
	l <- countTemp[1] (LO PARSO A DOUBLE)

	coupleNumber SPLITTO A ":" k[0]

	couple <- coupleNumber[0]
	n <- coupleNumber[1] (LO PARSO A DOUBLE)

	sum <- (l/count)*100
    sum2 <- (l/n)*100
    totSum <- sum+"%, "+sum2+"%"


	result <- Tuple2<couple, totSum>

RETURN result

SCORRO mapFoods PER CREARE UN MAPPA(Tuple2) finalResult
	KEY=mapFoods.key E VALUES= VALUES + " " + mapFoods.value

RETURN finalResult