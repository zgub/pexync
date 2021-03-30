# pexync

## TODO

1. fetch blockSize from viper

## Ideas

* destination reader moze pouzit **SectionReader** na rychlejsie paralelne vyratanie adler32 suctov ak bude __sectionSize nasobok blockSize__
* source reader **zda sa** nemoze pouzivat section reader, lebo robime rolling checksum a neviem ako vyriesit hranice sekcii, jedine ze by sme recyklovali sekcie, **sectionSize musi byt tiez nasobok blockSize**
* pouzivanie sekcii nema zmysel pri malych suboroch, tie nema asi zmysel ani robit diff, menej ako stovky bajtov
