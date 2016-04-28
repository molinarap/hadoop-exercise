# hadoop-exercise

### Project
Si supponga di avere a disposizione un file di testo generato da un sistema di billing di una catena di
supermercati che contiene, per ciascuno scontrino, una riga con la data e la lista dei prodotti
acquistati, separati da una virgola. Per esempio:
	```
	2015-03-21,uova,latte,pane,vino
	2015-05-18,pesce,pane,insalata,formaggio
	......
	```

Progettare e realizzare in: (a) MapReduce, (b) Hive e (c) Spark:
	
1. Un job che sia in grado di generare, per ciascun mese del 2015, i cinque prodotti più venduti seguiti dal numero complessivo di pezzi venduti. Per esempio:

	```
	2015-01: pane 852, latte 753, carne 544, vino 501, pesce 488
	2015-02: latte 744, burro 655, uova 585, birra 498, pane 457
	......
	```

2. Un job che, dato un file in un formato a piacere contenente il costo di ciascun prodotto, sia in grado di generare, per ciascun prodotto, l’incasso totale per quel prodotto di ciascun mese del 2015. Per esempio:

	```
	pane 1/2015:12340 2/2015:8530 3/2015:9450 …
	latte 1/2015:11987 2/2015:10980 3/2015:12350 …
	......
	```

3. Un job in grado di generare, per ciascuna coppia di prodotti (p1,p2): (i) la percentuale del numero complessivo di scontrini nei quali i due prodotti compaiono insieme (supporto della regola di associazione p1→p2) e (ii) la percentuale del numero di scontrini che contengono p1 nei quali compare anche p2 (confidenza della regola di associazione p1→p2)

	```
	pane,latte,30%, 4%
	vino,uova,23%, 4%
	latte,pane, 30%, 7%
	......
	```

### Project's libs

- **Download**: [http://www.java2s.com/Code/Jar/CatalogJar.htm]
- **Import** this libs in project:

	- hadoop-core
	- commons-cli
	- apache-logging-log4j
	- apache-commons-lang

### Init Project

1. clone project with: 
	```
	git clone
	```
2. create on Eclipse a *Maven Project*
3. add remote git repo
	```
	git remote add
	```
3. pull all repo
	```
	git pull origin pull
	```
