# PeXync

## Functionality

- parallel read and writes - configurable
- gzip compression
- local sync
- json + simple binary protocol over http
- Adler32 for block hash on the receiver side, rolling Adler32 on the sender side, sha1 used instead of MD4 compared to rsync

## Notes

- still a lot of duplicate code, especially between local and http receiver / sender as local sender / receiver were developed first to test concepts
- few benchmarks were performed to decide what readers to use, best compromise was to use buffered readers even with the section reader beneath
- the httpSender -(spawn)-> readers -(spawn)-> http sender goroutine design is cause by the initial local sender design, I would have to rewrite the local sender OR write a new dedicated sender as those readers expect a receiver channel, that's why there is another worker responsible just for the http clients. However I expect the disk I/o to be the slowest part on both sides, so this should not impact the performance much. It's just not very elegant, readable...

## Stretch goals / what could be added

- more clients / one sever
- continue after network outage

## What was avoided

- transfer encryption. My opinion regarding tools / utilities is "UX like", that a tool / utility should do **ONE** thing very well and leave the other specialized tools do their stuff the best way they can. My experience is that if a for example a data engineer writes encryption code, it's usually badly implemented standard. There are plenty of already existing https proxies I would use, instead of writing my own as I am not a (security / encryption ) developer, but more a devops guy that utilizes every existing handy tool around. Anyway it shouldn't be that difficult to implement though, with some already existing libraries, but it would be the same as using a proxy. Envoy / Nginx / Traefik and many more.
- server side data encryption - same reason. Anyway if I was implementing a secure backup, I'd rather create an encrypted FS or use encrypted storage, rather than using software encryption. Much safer.
