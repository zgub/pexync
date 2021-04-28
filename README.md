# PeXync

## Notes

- still much duplicate code, especially between local and http receiver / sender as local sender / receiver were developed first to test concepts
- few benchmarks were performed to decide what readers to use, best compromise was to use buffered readers even with the section reader beneath

## Stretch goals / what could be added

- more clients / one sever
- continue after network outage

## What was avoided

- transfer encryption. My idea is that a tool / utility should do **ONE** thing very well and leave the other specialized tools do their stuff the best way they can. My experience is that if a for example a data engineer writes encryption code, it's usually badly implemented standard. There are plenty of already existing https proxies I would use, instead of writing my own as I am not a (security / encryption ) developer, but more a devops guy that utilizes every existing handy tool around. Anyway it shouln't be that difficult to implement though with some already existing libraries, but it would be almost the same as using a proxy.
- server side data encryption - same reason. Anyway if I was implementing a secure backup, I'd rather create an encypted FS or use encryopted storage rather than using software encryption. Much safer.
