# dht-crawler

Minimal BitTorrent crawler and scheduler with RethinkDB backend to **collect, analyse and store** peers.

This crawler searches for peers of one or more torrents you define. Its core, forked from [Trrnts](https://github.com/Trrnts/Trrnts), searches for peers via the [DHT-Protocol](http://www.bittorrent.org/beps/bep_0005.html) and  sends ~4000 UDP packages per second. After, the crawler looks up the origin in a *GeoIP-database*, and stores it after.

#The stack
Node, trrnts (crawler core), RethinkDB

#The crawler
##The DHT protocol implementation
The BitTorrent DHT protocol is based on UDP, which is a connection-less, low-level protocol. The team from Trrnts made a pretty, straight-forward implementation of the protocol, however more abstract ways of DHT-crawling exist.

##Sample benchmark
This is a benchmark of a popular torrent as listed on [kickass.to](http://kickass.to)
![Sample benchmark](http://i.imgur.com/YkgClkU.png)
