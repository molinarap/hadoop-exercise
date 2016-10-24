
conf <- CREO UNA CONFIGARAZIONE SPARK
sc <- CREO UN CONTEXT SPARK

PRENDO IN INPUT UN FILE

CREO UNA RDD foods SPLITTANDO A "/n" PER LAVORARE SULLE RIGHE DEL FILE

SCORRO foods PER CREARE UN MAPPA(Tuple2) dateFoods

	allFood <- INIALIZZO UNA LISTA 

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
			AGGIUNGO all ALLA  LISTA allFood

RETURN allFood

RETURN dateFoods (UGUALE A allFood)

SCORRO dateFoods PER CREARE UN MAPPA(Tuple2) mapFoods
	KEY=dateFoods.key E VALUES=1

SCORRO mapFoods PER CREARE UN MAPPA(Tuple2) listFoods
	KEY=mapFoods.key E VALUES= VALUES + mapFoods.value

RETURN listFoods


SCORRO listFoods PER CREARE UN MAPPA(Tuple2) foodOrder

	line <- listFoods.element

	result <- TUPLE2

	prodDateNameTimeFalse SPLITTO A " "	line

	prodDate <- prodDateNameTimeFalse[0]
	prodNameTimeFalse <- prodDateNameTimeFalse[1]

	prodNameTime SPLITTO A "," prodNameTimeFalse 37,

	priceInMonth <- prodNameTime[0] + " " + prodNameTime[1]

	result <- Tuple2<prodDate, priceInMonth>

	RETURN result

SCORRO mapFoods PER CREARE UN MAPPA(Tuple2) result2
	KEY=mapFoods.key E VALUES= VALUES + ", " + mapFoods.value

RETURN result2