# pexync

## TODO

- [x] fetch blockSize from viper and overwrite calculated size
- [x] blocksize to be calculated
- [ ] study the rolling adler32
- [x] test the checksums
- [x] test various readers with paralelism
- [ ] decide when use single file send, single goroutine send, multiple goroutine send
- [ ] receiver should stop all go routines at RST
- [ ] do not create cfg file automaticaly, add a command **confgen**
- [ ] permissions / ownership / sha1 receiver comparison
- [ ] missing file sender
- [ ] multicore
- [ ] remove unnecessary pointers
- [ ] struct padding adjustment
- [ ] context.Done should send FIn to all workers

## Ideas

1. destination reader moze pouzit **SectionReader** na rychlejsie paralelne vyratanie adler32 suctov ak bude __sectionSize nasobok blockSize__
1. source reader **zda sa** nemoze pouzivat section reader, lebo robime rolling checksum a ~~neviem ako vyriesit hranice sekcii~~, jedine ze by sme recyklovali sekcie, **sectionSize musi byt tiez nasobok blockSize**
1 pouzivanie sekcii nema zmysel pri malych suboroch, tie nema asi zmysel ani robit diff, menej ako stovky bajtov
1. mime/multipart?
1. send blocks as they come
1. ctx, cancel = context.WithTimeout(context.Background(), timeout)
1. worker management? (in case one fails?)
1. Sha1 comparisson auto || on demand? (will require Sha1 hash calculation at first GetList call)

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
7. data stream headers

### sender reader

- fajl mensi ako 700 bytov je preneseny cely vzdy
  - ak cokolvek nesedi, posle sa cely
- fajl mensi ako 700 x 700 b ma blocksize 700
- fajl vacsi ako 490 000b ma blocksize = sqrt(size)
  - ak je fajl mensi ako 1GB **treba upresnit**, pouzije sa nebufferovany  

## Stretch goals

1. TLS
1. --delete
1. AAA

## Progress

1. checksum core concepts done, still have to understand the rolling adler32
1. let's first do the local sync and add remote later on

## Scenarios

- blockSize = 4
 N N M M M M
[       ] does not match, do nothing, adds to the oldBuff
  [       ] does not match, add N to buffer
    [       ] does match, add match to the buffer and seeks + blocksize
