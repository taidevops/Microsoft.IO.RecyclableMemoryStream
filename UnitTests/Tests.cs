namespace Microsoft.IO.UnitTests
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;

    using NUnit.Framework;

    /// <summary>
    /// Full test suite. It is abstract to allow parameters of the memory manager to be modified and tested in different
    /// combinations.
    /// </summary>
    public abstract class BaseRecyclableMemoryStreamTests
    {
        protected const int DefaultBlockSize = 16384;

        private readonly Random random = new Random();

        #region RecyclableMemoryManager Tests
        [Test]
        public virtual void RecyclableMemoryManagerUsingMultipleOrExponentialLargeBuffer()
        {
            var memMgr = this.GetMemoryManager();
        }

        [Test]
        public void RecyclableMemoryManagerThrowsExceptionOnZeroBlockSize()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new RecyclableMemoryStreamManager(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new RecyclableMemoryStreamManager(-1));
            Assert.DoesNotThrow(() => new RecyclableMemoryStreamManager(1));
        }



        [Test]
        public void GettingBlockAdjustsFreeAndInUseSize()
        {
            var memMgr = this.GetMemoryManager();
            Assert.That(memMgr.SmallPoolFreeSize, Is.EqualTo(0));
            Assert.That(memMgr.SmallPoolInUseSize, Is.EqualTo(0));

            // This should create a new block
            var block = memMgr.GetBlock();

            Assert.That(memMgr.SmallPoolFreeSize, Is.EqualTo(0));
            Assert.That(memMgr.SmallPoolInUseSize, Is.EqualTo(memMgr.BlockSize));

            memMgr.ReturnBlocks(new List<byte[]> { block }, Guid.Empty, string.Empty);

            Assert.That(memMgr.SmallPoolFreeSize, Is.EqualTo(memMgr.BlockSize));
            Assert.That(memMgr.SmallPoolInUseSize, Is.EqualTo(0));

            // This should get an existing block
            block = memMgr.GetBlock();

            Assert.That(memMgr.SmallPoolFreeSize, Is.EqualTo(0));
            Assert.That(memMgr.SmallPoolInUseSize, Is.EqualTo(memMgr.BlockSize));

            memMgr.ReturnBlocks(new List<byte[]> { block }, Guid.Empty, string.Empty);

            Assert.That(memMgr.SmallPoolFreeSize, Is.EqualTo(memMgr.BlockSize));
            Assert.That(memMgr.SmallPoolInUseSize, Is.EqualTo(0));
        }
        #endregion

        #region Test Helpers
        protected virtual RecyclableMemoryStreamManager GetMemoryManager()
        {
            return new RecyclableMemoryStreamManager(DefaultBlockSize);
        }

        protected byte[] GetRandomBuffer(int length)
        {
            var buffer = new byte[length];
            random.NextBytes(buffer);
            return buffer;
        }
        #endregion
    }

    [TestFixture]
    public sealed class RecyclableMemoryStreamTestsWithPassiveBufferRelease : BaseRecyclableMemoryStreamTests
    {
    }
}
