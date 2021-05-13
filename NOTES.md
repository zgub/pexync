# pexync

## TODO

- [x] fetch blockSize from viper and overwrite calculated size
- [x] blocksize to be calculated
- [x] study the rolling adler32
- [x] test the checksums
- [x] test various readers with paralelism
- [ ] decide when use single file send, single goroutine send, multiple goroutine send
- [ ] receiver should stop all go routines at RST
- [x] do not create cfg file automaticaly, add a command **confgen**
- [ ] permissions / ownership / sha1 receiver comparison
- [x] missing file sender
- [ ] multicore
- [ ] remove unnecessary pointers
- [ ] struct padding adjustment
- [ ] context.Done should send FIN to all workers
- [ ] change only meta if only meta was changed add **META** flag!!!
- [ ] UUID and AAA
- [ ] contexts
- [x] switch local / remote
- [ ] fail when remote host is not specified
- [ ] validate inputs
- [ ] do I need fileindex????? (WHOA) seem not
- [ ] tests!
- [ ] validation at transfer end
- [ ] sender state (reset after aaa)
- [x] make sure to not send the same file
- [ ] anonymous helper functions
- [ ] don't use w for workers as w implies writer

## NOTODO

- [ ] proto checksums?

## Ideas

1. destination reader moze pouzit **SectionReader** na rychlejsie paralelne vyratanie adler32 suctov ak bude __sectionSize nasobok blockSize__
1. source reader **zda sa** nemoze pouzivat section reader, lebo robime rolling checksum a ~~neviem ako vyriesit hranice sekcii~~, jedine ze by sme recyklovali sekcie, **sectionSize musi byt tiez nasobok blockSize**
1. pouzivanie sekcii nema zmysel pri malych suboroch, tie nema asi zmysel ani robit diff, menej ako stovky bajtov
1. mime/multipart?
1. send blocks as they come
1. ctx, cancel = context.WithTimeout(context.Background(), timeout)
1. ~~worker management? (in case one fails?)~~ not in first version
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
