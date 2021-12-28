namespace Microsoft.IO
{
    using System;

    public sealed partial class RecyclableMemoryStreamManager
    {
        /// <summary>
        /// Arguments for the UsageReport event.
        /// </summary>
        public sealed class UsageReportEventArgs : EventArgs
        {
            /// <summary>
            /// Bytes from the small pool currently in use.
            /// </summary>
            public long SmallPoolInUseBytes { get; }

            /// <summary>
            /// Initializes a new instance of the <see cref="UsageReportEventArgs"/> class.
            /// </summary>
            /// <param name="smallPoolInUseBytes">Bytes from the small pool currently in use.</param>
            public UsageReportEventArgs(
                long smallPoolInUseBytes)
            {
                this.SmallPoolInUseBytes = smallPoolInUseBytes;
            }
        }
    }
}
