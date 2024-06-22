# My attempt at the 1 Billion Row Challenge
*OP: https://github.com/gunnarmorling/1brc*

## TL;DR
- I/O via `RandomAccess`
- `stackalloc` for I/O read buffers
- No unsafe. Only `Span`s
- Dictionary capacity tweaking (x20 expected capacity seems optimal)
- Read buffer size tweaking (64KB seems optimal)
- Very naive Integer temperature parsing
- Very naive City name parsing. If any name contains a unicode character that has the byte value of a semi-colon, this breaks.
- Blatant stealing of Victor Baybekov's hashing function ( see https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/ )
- Little to no mechanical sympathy. What are cache-lines?


Seems to be on-par with the more fancy attempts:

https://github.com/nietras/1brc.cs 's attempt

    $ hyperfine 'build/bin/Brc/Release/net8.0/linux-x64/Brc /tmp/ramdisk/measurements.txt'
    Benchmark 1: build/bin/Brc/Release/net8.0/linux-x64/Brc /tmp/ramdisk/measurements.txt
    Time (mean ± σ):     922.9 ms ±  15.5 ms    [User: 13081.8 ms, System: 1300.9 ms]
    Range (min … max):   903.8 ms … 952.5 ms    10 runs

My attempt:

    $ hyperfine './bin/Release/net8.0/linux-x64/publish/1brc'
    Benchmark 1: ./bin/Release/net8.0/linux-x64/publish/1brc
    Time (mean ± σ):     919.5 ms ±   2.6 ms    [User: 26737.7 ms, System: 1706.2 ms]
    Range (min … max):   917.7 ms … 926.2 ms    10 runs

... as run on my machine ( AMD Ryzen 9 5950X, 16-cores, 32-threads )