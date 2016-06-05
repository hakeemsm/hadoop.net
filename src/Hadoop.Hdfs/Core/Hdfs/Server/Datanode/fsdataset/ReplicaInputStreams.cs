using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>Contains the input streams for the data and checksum of a replica.</summary>
	public class ReplicaInputStreams : IDisposable
	{
		private readonly InputStream dataIn;

		private readonly InputStream checksumIn;

		private readonly FsVolumeReference volumeRef;

		/// <summary>Create an object with a data input stream and a checksum input stream.</summary>
		public ReplicaInputStreams(InputStream dataStream, InputStream checksumStream, FsVolumeReference
			 volumeRef)
		{
			this.volumeRef = volumeRef;
			this.dataIn = dataStream;
			this.checksumIn = checksumStream;
		}

		/// <returns>the data input stream.</returns>
		public virtual InputStream GetDataIn()
		{
			return dataIn;
		}

		/// <returns>the checksum input stream.</returns>
		public virtual InputStream GetChecksumIn()
		{
			return checksumIn;
		}

		public virtual void Close()
		{
			IOUtils.CloseStream(dataIn);
			IOUtils.CloseStream(checksumIn);
			IOUtils.Cleanup(null, volumeRef);
		}
	}
}
