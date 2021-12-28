namespace Microsoft.IO
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// MemoryStream implementation that deals with pooling and managing memory streams which use potentially large
    /// </summary>
    public sealed class RecyclableMemoryStream
    {
        private readonly Guid id;

        private readonly RecyclableMemoryStreamManager memoryManager;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="RecyclableMemoryStream"/> class.
        /// </summary>
        /// <param name="memoryManager">The memory manager.</param> 
        public RecyclableMemoryStream(RecyclableMemoryStreamManager memoryManager)
            : this(memoryManager, Guid.NewGuid()) { }
        
        internal RecyclableMemoryStream(RecyclableMemoryStreamManager memoryManager, Guid id)
        {
            this.memoryManager = memoryManager;
            this.id = id;
        }
        #endregion

        public void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset,
                    $"{nameof(offset)} must be in the range of 0 - {nameof(buffer)}.{nameof(buffer.Length)}-1.");
            }
            
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, $"{nameof(count)} must be non-negative.");
            }

            if (count + offset > buffer.Length)
            {
                throw new ArgumentException($"{nameof(count)} must be greater than {nameof(buffer)}.{nameof(buffer.Length)} - {nameof(offset)}.");
            }

            Buffer.BlockCopy(buffer, offset, new byte[] {}, 0, count);
        }
    }
}
