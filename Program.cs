using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

string fileName = "/tmp/ramdisk/measurements.txt";
var fileHandle = OpenFileHandle(fileName);
var fileSize = RandomAccess.GetLength(fileHandle);

int CORE_COUNT = Environment.ProcessorCount;
int CHUNK_COUNT = Environment.ProcessorCount;
long CHUNK_SIZE = fileSize / CHUNK_COUNT;
int DICT_INIT_CAPACITY = 20000; // Using a capacity many times (~ x20) larger than what we expect seems to be the sweet spot
int BUFFER_SIZE = 64 * 1024;

var result = SplitIntoChunks(CHUNK_COUNT, CHUNK_SIZE, fileHandle, fileSize)
    .AsParallel()
    .WithDegreeOfParallelism(CORE_COUNT)
    .Select(ProcessChunk)
    .Aggregate(new Dictionary<Utf8Span, Summary>(),
        (acc, map) =>
        {
            foreach (var pair in map)
            {
                ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(acc, pair.Key, out bool exists);
                if (!exists)
                {
                    summary = pair.Value;
                }
                else
                {
                    summary.MergeInto(pair.Value);
                }
            }
            return acc;
        });

foreach (var item in result.OrderBy(x => x.Value.City))
{
    Console.Write($"{item.Value.City}={item.Value.CalculateMin:F1}/{item.Value.CalculateAvg:F1}/{item.Value.CalculateMax:F1}, ");
}
Console.WriteLine();
Console.WriteLine(result.Count);

Dictionary<Utf8Span, Summary> ProcessChunk((long start, long length) range)
{
    var map = new Dictionary<Utf8Span, Summary>(DICT_INIT_CAPACITY);
    Span<byte> buffer = stackalloc byte[BUFFER_SIZE];

    long pointer = range.start;
    long chunkEnd = range.start + range.length;
    
    int read = RandomAccess.Read(fileHandle, buffer, pointer);
    while (read > 0 && pointer < chunkEnd)
    {
        pointer += read;

        Span<byte> slice = buffer.Slice(0, read);
        while (slice.Length > 0)
        {
            int semiColonIndex = slice.IndexOf((byte)';');
            if (semiColonIndex > -1)
            {
                Span<byte> tempSlice = slice.Slice(semiColonIndex + 1);
                var newLineIndex = tempSlice.IndexOf((byte)'\n');
                if (newLineIndex > -1 || pointer == chunkEnd)
                {
                    var citySpan = slice.Slice(0, semiColonIndex);
                    ref var summary = ref CollectionsMarshal.GetValueRefOrAddDefault(map, new Utf8Span(citySpan), out bool exists);
                    if (!exists)
                    {
                        summary = new Summary(Encoding.UTF8.GetString(citySpan));
                    }
                    short measurement = ParseTemp(tempSlice.Slice(0, newLineIndex));
                    summary.Apply(measurement);
                    slice = tempSlice.Slice(newLineIndex + 1);
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
        
        // Set pointer back a bit so we start at a next segment
        pointer -= slice.Length;

        if (pointer < chunkEnd)
        {
            read = RandomAccess.Read(fileHandle, buffer, pointer);
        }
    }

    return map;
}

static List<(long start, long length)> SplitIntoChunks(int chunkCount, long chunkSize, SafeFileHandle fileHandle, long fileSize)
{
    List<(long start, long length)> results = new();
    
    Span<byte> buffer = stackalloc byte[50];
    long offset = 0;
    for (int i = 0; i < chunkCount - 1; i++)
    {
        long end = offset + chunkSize;
        var slice = buffer.Slice(0, RandomAccess.Read(fileHandle, buffer, end));
        int index = slice.IndexOf((byte)'\n');
        Debug.Assert(index > -1);
        end += index + 1;
        results.Add((offset, end - offset));
        offset = end;
    }
    results.Add((offset, fileSize - offset));
    return results;
}

[MethodImpl(MethodImplOptions.AggressiveInlining)]
static short ParseTemp(Span<byte> span)
{
    int result = 0, sign = 1;
    if (span[0] == '-')
    {
        span = span.Slice(1);
        sign = -1;
    }

    if (span.Length == 4)
    {
        result = 100 * (span[0] - '0') + 10 * (span[1] - '0');
    }
    else
    {
        result = 10 * (span[0] - '0');
    }

    var fraction = span[^1] - '0';
    return (short)(sign * (result + fraction));
}

static SafeFileHandle OpenFileHandle(string filePath, FileOptions fileOptions = FileOptions.None) =>
    File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, fileOptions);

struct Utf8Span : IEquatable<Utf8Span>
{
    readonly int _hash;
    public Utf8Span(ReadOnlySpan<byte> span)
    {
        _hash = CalculateHash(span);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int CalculateHash(ReadOnlySpan<byte> read)
    {
        const uint prime = 16777619u;
        if (read.Length > 3)
        {
            return (int)(((uint)read.Length * prime) ^ MemoryMarshal.Read<uint>(read.Slice(0, 4)));
        }
        return MemoryMarshal.Read<ushort>(read.Slice(0, 2)) * 31;
    }

    public bool Equals(Utf8Span other)
    {
        return _hash == other._hash;
    }

    public override bool Equals(object? obj)
    {
        return obj is Utf8Span other && Equals(other);
    }

    public override int GetHashCode()
    {
        return _hash;
    }
}

struct Summary(string city)
{
    public string City = city;
    public ulong Count;
    public long Temp;
    public short Min;
    public short Max;
    public float CalculateAvg => (float)Temp / Count / 10;
    public float CalculateMin => (float)Min / 10;
    public float CalculateMax => (float)Max / 10;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Apply(short measurement)
    {
        Count++;
        Temp += measurement;
        Max = Max < measurement ? measurement : Max;
        Min = Min > measurement ? measurement : Min;
    }

    public void MergeInto(Summary summary)
    {
        Count += summary.Count;
        Temp += summary.Temp;
        Max = Max < summary.Max ? summary.Max : Max;
        Min = Min > summary.Min ? summary.Min : Min;
        City ??= summary.City;
    }
}