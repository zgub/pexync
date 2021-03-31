# pexync

## TODO

1. fetch blockSize from viper

## Ideas

* destination reader moze pouzit **SectionReader** na rychlejsie paralelne vyratanie adler32 suctov ak bude __sectionSize nasobok blockSize__
* source reader **zda sa** nemoze pouzivat section reader, lebo robime rolling checksum a neviem ako vyriesit hranice sekcii, jedine ze by sme recyklovali sekcie, **sectionSize musi byt tiez nasobok blockSize**
* pouzivanie sekcii nema zmysel pri malych suboroch, tie nema asi zmysel ani robit diff, menej ako stovky bajtov

## Design concepts

1. connect
2. auth?
3. sender posle filelist, alebo posiela priebezne
4. receiver dostane filelist a porovna ho s tym co ma
    1. worker pool dostane sekcie a bude vracat checksumy
    2. collector worker ich spravne zaradi k suborom
    3. vygeneruje sa checksum list pre kazdy subok
    4. posle sa senderovi
5. ak --delete zmaze nesync data
6. pre kazdy file vznikne jedna alebo dve goroutinny

### sender reader

* fajl mensi ako 700 bytov je preneseny cely vzdy
  * ak cokolvek nesedi, posle sa cely
* fajl mensi ako 700 x 700 b ma blocksize 700
* fajl vacsi ako 490 000b ma blocksize = sqrt(size)
  * ak je fajl mensi ako 1GB **treba upresnit**, pouzije sa nebufferovany  

## Stretch goals

1. TLS
2. --delete
