namespace Microsoft.IO
{
    using System;
    using System.Diagnostics.Tracing;

    public sealed partial class RecyclableMemoryStreamManager
    {
        /// <summary>
        /// ETW events for RecyclableMemoryStream.
        /// </summary>
        [EventSource(Name = "Microsoft-IO-RecyclableMemoryStream", Guid = "{B80CD4E4-890E-468D-9CBA-90EB7C82DFC7}")]
        public sealed class Events : EventSource
        {
            /// <summary>
            /// Static log object, through which all events are written.
            /// </summary>
            public static Events Writer = new Events();


        }
    }
}
