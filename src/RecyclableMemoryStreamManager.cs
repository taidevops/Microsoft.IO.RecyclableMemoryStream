namespace Microsoft.IO
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Threading;

    /// <summary>
    /// Manages pools of <see cref="RecyclableMemoryStream"/> objects.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There are two pools managed in here. The small pool contains same-sized buffers that are handed to streams
    /// as they write more data.
    ///</para>
    ///<para>
    /// For scenarios that need to call <see cref="RecyclableMemoryStream.GetBuffer"/>, the large pool contains buffers of various sizes, all
    /// multiples/exponentials of <see cref="LargeBufferMultiple"/> (1 MB by default). They are split by size to avoid overly-wasteful buffer
    /// usage. There should be far fewer 8 MB buffers than 1 MB buffers, for example.
    /// </para>
    /// </remarks>
    public partial class RecyclableMemoryStreamManager
    {
        /// <summary>
        /// Maximum length of a single array.
        /// </summary>
        /// <remarks>See documentation at https://docs.microsoft.com/dotnet/api/system.array?view=netcore-3.1
        /// </remarks>
        internal const int MaxArrayLength = 0X7FFFFFC7;

        /// <summary>
        /// Default block size, in bytes.
        /// </summary>
        public const int DefaultBlockSize = 128 * 1024;

        /// <summary>
        /// Default large buffer multiple, in bytes.
        /// </summary>
        public const int DefaultLargeBufferMultiple = 1024 * 1024;

        /// <summary>
        /// Default maximum buffer size, in bytes.
        /// </summary>
        public const int DefaultMaximumBufferSize = 128 * 1024 * 1024;

        // 0 to indicate unbounded
        private const long DefaultMaxSmallPoolFreeBytes = 0L;
        private const long DefaultMaxLargePoolFreeBytes = 0L;

        private readonly long[] largeBufferFreeSize;
        private readonly long[] largeBufferInUseSize;

        private readonly ConcurrentStack<byte[]>[] largePools;

        private readonly ConcurrentStack<byte[]> smallPool;

        private long smallPoolFreeSize;
        private long smallPoolInUseSize;

        /// <summary>
        /// Initializes the memory manager with the default block/buffer specifications. This pool may have unbounded growth unless you modify <see cref="MaximumFreeSmallPoolBytes"/> and <see cref="MaximumFreeLargePoolBytes"/>.
        /// </summary>
        public RecyclableMemoryStreamManager()
            : this(DefaultBlockSize, DefaultLargeBufferMultiple, DefaultMaximumBufferSize, false, DefaultMaxSmallPoolFreeBytes, DefaultMaxLargePoolFreeBytes) { }

        /// <summary>
        /// Initializes the memory manager with the default block/buffer specifications and maximum free bytes specifications.
        /// </summary>
        /// <param name="maximumSmallPoolFreeBytes">Maximum number of bytes to keep available in the small pool before future buffers get dropped for garbage collection</param>
        /// <param name="maximumLargePoolFreeBytes">Maximum number of bytes to keep available in the large pool before future buffers get dropped for garbage collection</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumSmallPoolFreeBytes"/> is negative, or <paramref name="maximumLargePoolFreeBytes"/> is negative.</exception>
        public RecyclableMemoryStreamManager(long maximumSmallPoolFreeBytes, long maximumLargePoolFreeBytes)
            : this(DefaultBlockSize, DefaultLargeBufferMultiple, DefaultMaximumBufferSize, useExponentialLargeBuffer: false, maximumSmallPoolFreeBytes, maximumLargePoolFreeBytes)
        {
        }

        /// <summary>
        /// Initializes the memory manager with the given block requiredSize. This pool may have unbounded growth unless you modify <see cref="MaximumFreeSmallPoolBytes"/> and <see cref="MaximumFreeLargePoolBytes"/>.
        /// </summary>
        /// <param name="blockSize">Size of each block that is pooled. Must be > 0.</param>
        /// <param name="largeBufferMultiple">Each large buffer will be a multiple of this value.</param>
        /// <param name="maximumBufferSize">Buffers larger than this are not pooled</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="blockSize"/> is not a positive number,
        /// or <paramref name="largeBufferMultiple"/> is not a positive number,
        /// or <paramref name="maximumBufferSize"/> is less than <paramref name="blockSize"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="maximumBufferSize"/> is not a multiple of <paramref name="largeBufferMultiple"/>.</exception>
        public RecyclableMemoryStreamManager(int blockSize, int largeBufferMultiple, int maximumBufferSize)
            : this(blockSize, largeBufferMultiple, maximumBufferSize, false, DefaultMaxSmallPoolFreeBytes, DefaultMaxLargePoolFreeBytes) { }

        /// <summary>
        /// Initializes the memory manager with the given block requiredSize.
        /// </summary>
        /// <param name="blockSize">Size of each block that is pooled. Must be > 0.</param>
        /// <param name="largeBufferMultiple">Each large buffer will be a multiple of this value.</param>
        /// <param name="maximumBufferSize">Buffers larger than this are not pooled</param>
        /// <param name="maximumSmallPoolFreeBytes">Maximum number of bytes to keep available in the small pool before future buffers get dropped for garbage collection</param>
        /// <param name="maximumLargePoolFreeBytes">Maximum number of bytes to keep available in the large pool before future buffers get dropped for garbage collection</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="blockSize"/> is not a positive number,
        /// or <paramref name="largeBufferMultiple"/> is not a positive number,
        /// or <paramref name="maximumBufferSize"/> is less than <paramref name="blockSize"/>,
        /// or <paramref name="maximumSmallPoolFreeBytes"/> is negative,
        /// or <paramref name="maximumLargePoolFreeBytes"/> is negative.
        /// </exception>
        /// <exception cref="ArgumentException"><paramref name="maximumBufferSize"/> is not a multiple of <paramref name="largeBufferMultiple"/>.</exception>
        public RecyclableMemoryStreamManager(int blockSize, int largeBufferMultiple, int maximumBufferSize, long maximumSmallPoolFreeBytes, long maximumLargePoolFreeBytes)
            : this(blockSize, largeBufferMultiple, maximumBufferSize, false, maximumSmallPoolFreeBytes, maximumLargePoolFreeBytes) { }


        /// <summary>
        /// Initializes the memory manager with the given block requiredSize. This pool may have unbounded growth unless you modify <see cref="MaximumFreeSmallPoolBytes"/> and <see cref="MaximumFreeLargePoolBytes"/>.
        /// </summary>
        /// <param name="blockSize">Size of each block that is pooled. Must be > 0.</param>
        /// <param name="largeBufferMultiple">Each large buffer will be a multiple/exponential of this value.</param>
        /// <param name="maximumBufferSize">Buffers larger than this are not pooled</param>
        /// <param name="useExponentialLargeBuffer">Switch to exponential large buffer allocation strategy</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="blockSize"/> is not a positive number,
        /// or <paramref name="largeBufferMultiple"/> is not a positive number,
        /// or <paramref name="maximumBufferSize"/> is less than <paramref name="blockSize"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="maximumBufferSize"/> is not a multiple/exponential of <paramref name="largeBufferMultiple"/>.</exception>
        public RecyclableMemoryStreamManager(int blockSize, int largeBufferMultiple, int maximumBufferSize, bool useExponentialLargeBuffer)
            : this(blockSize, largeBufferMultiple, maximumBufferSize, useExponentialLargeBuffer, DefaultMaxSmallPoolFreeBytes, DefaultMaxLargePoolFreeBytes)
        {
        }

        /// <summary>
        /// Initializes the memory manager with the given block requiredSize.
        /// </summary>
        /// <param name="blockSize">Size of each block that is pooled. Must be > 0.</param>
        /// <param name="largeBufferMultiple">Each large buffer will be a multiple/exponential of this value.</param>
        /// <param name="maximumBufferSize">Buffers larger than this are not pooled.</param>
        /// <param name="useExponentialLargeBuffer">Switch to exponential large buffer allocation strategy.</param>
        /// <param name="maximumSmallPoolFreeBytes">Maximum number of bytes to keep available in the small pool before future buffers get dropped for garbage collection.</param>
        /// <param name="maximumLargePoolFreeBytes">Maximum number of bytes to keep available in the large pool before future buffers get dropped for garbage collection.</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="blockSize"/> is not a positive number,
        /// or <paramref name="largeBufferMultiple"/> is not a positive number,
        /// or <paramref name="maximumBufferSize"/> is less than <paramref name="blockSize"/>,
        /// or <paramref name="maximumSmallPoolFreeBytes"/> is negative,
        /// or <paramref name="maximumLargePoolFreeBytes"/> is negative.
        /// </exception>
        /// <exception cref="ArgumentException"><paramref name="maximumBufferSize"/> is not a multiple/exponential of <paramref name="largeBufferMultiple"/>.</exception>
        public RecyclableMemoryStreamManager(int blockSize, int largeBufferMultiple, int maximumBufferSize, bool useExponentialLargeBuffer, long maximumSmallPoolFreeBytes, long maximumLargePoolFreeBytes)
        {
            if (blockSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, $"{nameof(blockSize)} must be a positive number");
            }

            if (largeBufferMultiple <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(largeBufferMultiple), $"{nameof(largeBufferMultiple)} must be a positive number");
            }

            if (maximumBufferSize < blockSize)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumBufferSize), $"{nameof(maximumBufferSize)} must be at least {nameof(blockSize)}");
            }

            if (maximumSmallPoolFreeBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumSmallPoolFreeBytes), $"{nameof(maximumSmallPoolFreeBytes)} must be non-negative");
            }

            if (maximumLargePoolFreeBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumLargePoolFreeBytes), $"{nameof(maximumLargePoolFreeBytes)} must be non-negative");
            }

            this.BlockSize = blockSize;
            this.LargeBufferMultiple = largeBufferMultiple;
            this.MaximumBufferSize = maximumBufferSize;
            this.UseExponentialLargeBuffer = useExponentialLargeBuffer;
            this.MaximumFreeSmallPoolBytes = maximumSmallPoolFreeBytes;
            this.MaximumFreeLargePoolBytes = maximumLargePoolFreeBytes;

            if (!this.IsLargeBufferSize(maximumBufferSize))
            {
                throw new ArgumentException(
                    $"{nameof(maximumBufferSize)} is not {(this.UseExponentialLargeBuffer ? "an exponential" : "a multiple")} of {nameof(largeBufferMultiple)}.",
                    nameof(maximumBufferSize));
            }

            this.smallPool = new ConcurrentStack<byte[]>();
            var numLargePools = useExponentialLargeBuffer
                                    ? ((int)Math.Log(maximumBufferSize / largeBufferMultiple, 2) + 1)
                                    : (maximumBufferSize / largeBufferMultiple);

            // +1 to store size of bytes in use that are too large to be pooled
            this.largeBufferInUseSize = new long[numLargePools + 1];
            this.largeBufferFreeSize = new long[numLargePools];

            this.largePools = new ConcurrentStack<byte[]>[numLargePools];

            for (var i = 0; i < this.largePools.Length; ++i)
            {
                this.largePools[i] = new ConcurrentStack<byte[]>();
            }

            Events.Writer.MemoryStreamManagerInitialized(blockSize, largeBufferMultiple, maximumBufferSize);
        }

        /// <summary>
        /// The size of each block. It must be set at creation and cannot be changed.
        /// </summary>
        public int BlockSize { get; }

        /// <summary>
        /// All buffers are multiples/exponentials of this number. It must be set at creation and cannot be changed.
        /// </summary>
        public int LargeBufferMultiple { get; }

        /// <summary>
        /// Use multiple large buffer allocation strategy. It must be set at creation and cannot be changed.
        /// </summary>
        public bool UseMultipleLargeBuffer => !this.UseExponentialLargeBuffer;

        /// <summary>
        /// Use exponential large buffer allocation strategy. It must be set at creation and cannot be changed.
        /// </summary>
        public bool UseExponentialLargeBuffer { get; }

        /// <summary>
        /// Gets the maximum buffer size.
        /// </summary>
        /// <remarks>Any buffer that is returned to the pool that is larger than this will be
        /// discarded and garbage collected.</remarks>
        public int MaximumBufferSize { get; }

        /// <summary>
        /// Number of bytes in small pool not currently in use.
        /// </summary>
        public long SmallPoolFreeSize => this.smallPoolFreeSize;

        /// <summary>
        /// Number of bytes currently in use by stream from the small pool.
        /// </summary>
        public long SmallPoolInUseSize => this.smallPoolInUseSize;

        /// <summary>
        /// Number of bytes in large pool not currently in use.
        /// </summary>
        public long LargePoolFreeSize
        {
            get
            {
                long sum = 0;
                foreach (long freeSize in this.largeBufferFreeSize)
                {
                    sum += freeSize;
                }

                return sum;
            }
        }

        /// <summary>
        /// Number of bytes currently in use by streams from the large pool.
        /// </summary>
        public long LargePoolInUseSize
        {
            get
            {
                long sum = 0;
                foreach (long inUseSize in this.largeBufferInUseSize)
                {
                    sum += inUseSize;
                }

                return sum;
            }
        }

        /// <summary>
        /// How many blocks are in the small pool.
        /// </summary>
        public long SmallBlocksFree => this.smallPool.Count;

        /// <summary>
        /// How many buffers are in the large pool.
        /// </summary>
        public long LargeBuffersFree
        {
            get
            {
                long free = 0;
                foreach (var pool in this.largePools)
                {
                    free += pool.Count;
                }
                return free;
            }
        }

        /// <summary>
        /// How many bytes of small free blocks to allow before we start dropping
        /// those returned to us.
        /// </summary>
        /// <remarks>The default value is 0, meaning the pool is unbounded.</remarks>
        public long MaximumFreeSmallPoolBytes { get; set; }

        /// <summary>
        /// How many bytes of large free buffers to allow before we start dropping
        /// those returned to us.
        /// </summary>
        /// <remarks>The default value is 0, meaning the pool is unbounded.</remarks>
        public long MaximumFreeLargePoolBytes { get; set; }

        /// <summary>
        /// Maximum stream capacity in bytes. Attempts to set a larger capacity will
        /// result in an exception.
        /// </summary>
        /// <remarks>A value of 0 indicates no limit.</remarks>
        public long MaximumStreamCapacity { get; set; }

        /// <summary>
        /// Whether to save callstacks for stream allocations. This can help in debugging.
        /// It should NEVER be turned on generally in production.
        /// </summary>
        public bool GenerateCallStacks { get; set; }

        /// <summary>
        /// Whether dirty buffers can be immediately returned to the buffer pool.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When <see cref="RecyclableMemoryStream.GetBuffer"/> is called on a stream and creates a single large buffer, if this setting is enabled, the other blocks will be returned
        /// to the buffer pool immediately.
        /// </para>
        /// <para>
        /// Note when enabling this setting that the user is responsible for ensuring that any buffer previously
        /// retrieved from a stream which is subsequently modified is not used after modification (as it may no longer
        /// be valid).
        /// </para>
        /// </remarks>
        public bool AggressiveBufferReturn { get; set; }

        /// <summary>
        /// Causes an exception to be thrown if <see cref="RecyclableMemoryStream.ToArray"/> is ever called.
        /// </summary>
        /// <remarks>Calling <see cref="RecyclableMemoryStream.ToArray"/> defeats the purpose of a pooled buffer. Use this property to discover code that is calling <see cref="RecyclableMemoryStream.ToArray"/>. If this is
        /// set and <see cref="RecyclableMemoryStream.ToArray"/> is called, a <c>NotSupportedException</c> will be thrown.</remarks>
        public bool ThrowExceptionOnToArray { get; set; }

        /// <summary>
        /// Removes and returns a single block from the pool.
        /// </summary>
        /// <returns>A <c>byte[]</c> array.</returns>
        internal byte[] GetBlock()
        {
            Interlocked.Add(ref this.smallPoolInUseSize, this.BlockSize);

            if (!this.smallPool.TryPop(out byte[] block))
            {
                // We'll add this back to the pool when the stream is disposed
                // (unless our free pool is too large)
#if NET5_0_OR_GREATER
                block = GC.AllocateUninitializedArray<byte>(this.BlockSize);
#else
                block = new byte[this.BlockSize];
#endif

            }
            else
            {
                Interlocked.Add(ref this.smallPoolFreeSize, -this.BlockSize);
            }

            return block;
        }

        private long RoundToLargeBufferSize(long requiredSize)
        {
            if (this.UseExponentialLargeBuffer)
            {
                long pow = 1;
                while (this.LargeBufferMultiple * pow < requiredSize)
                {
                    pow <<= 1;
                }
                return this.LargeBufferMultiple * pow;
            }
            else
            {
                return ((requiredSize + this.LargeBufferMultiple - 1) / this.LargeBufferMultiple) * this.LargeBufferMultiple;
            }
        }

        private bool IsLargeBufferSize(int value)
        {
            return (value != 0) && (this.UseExponentialLargeBuffer
                                        ? (value == RoundToLargeBufferSize(value))
                                        : (value % this.LargeBufferMultiple) == 0);
        }

        /// <summary>
        /// Returns the blocks to the pool.
        /// </summary>
        /// <param name="blocks">Collection of blocks to return to the pool.</param>
        /// <param name="id">Unique Stream ID.</param>
        /// <param name="tag">The tag of the stream returning these blocks, for logging if necessary.</param>
        /// <exception cref="ArgumentNullException"><paramref name="blocks"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="blocks"/> contains buffers that are the wrong size (or null) for this memory manager.</exception>
        internal void ReturnBlocks(List<byte[]> blocks, Guid id, string tag)
        {
            if (blocks == null)
            {
                throw new ArgumentNullException(nameof(blocks));
            }

            long bytesToReturn = (long)blocks.Count * (long)this.BlockSize;
            Interlocked.Add(ref this.smallPoolInUseSize, -bytesToReturn);

            foreach (var block in blocks)
            {
                if (block == null || block.Length != this.BlockSize)
                {
                    throw new ArgumentException($"{nameof(blocks)} contains buffers that are not {nameof(BlockSize)} in length.", nameof(blocks));
                }
            }

            foreach (var block in blocks)
            {
                if (this.MaximumFreeSmallPoolBytes == 0 || this.SmallPoolFreeSize < this.MaximumFreeSmallPoolBytes)
                {
                    Interlocked.Add(ref this.smallPoolFreeSize, this.BlockSize);
                    this.smallPool.Push(block);
                }
                else
                {
                    break;
                }
            }

            ReportUsageReport(this.smallPoolInUseSize);
        }

        internal void ReportUsageReport(long smallPoolInUseBytes)
        {
            this.UsageReport?.Invoke(this, new UsageReportEventArgs(smallPoolInUseBytes));
        }

        /// <summary>
        /// Periodically triggered to report usage statistics.
        /// </summary>
        public event EventHandler<UsageReportEventArgs> UsageReport;
    }
}
