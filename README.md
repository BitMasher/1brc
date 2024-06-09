| attempt | time                 | machine             | notes                                                                                                             |
|---------|----------------------|---------------------|-------------------------------------------------------------------------------------------------------------------|
| 1       | 156.552 s ±  0.769 s | Mac Studio m2 Ultra | initial implementation, single threaded                                                                           |
| 2       | 151                  | Mac Studio m2 Ultra | removed all the safety tests                                                                                      |
| 3       | 115.677 s ±  0.818 s | Mac Studio m2 Ultra | Set reader capacity to 4k, got rid of excess copies, switched to enum pattern matching instead of global city var |
| 4       | 8.111 s ±  0.311 s   | Mac Studio m2 Ultra | Multi-threaded (cores * 2), ramdisk                                                                               |
| 5       | 7.741 s ±  0.250 s   | Mac Studio m2 Ultra | Read entire file segment at once                                                                                  |
| 6       | 6.851 s ±  0.226 s   | Mac Studio m2 Ultra | removed a vector copy on city name                                                                                |
| 7       | 6.506 s ±  0.158 s   | Mac Studio m2 Ultra | switch hashing function for hashmap and changed key from string to [u8]                                           |
