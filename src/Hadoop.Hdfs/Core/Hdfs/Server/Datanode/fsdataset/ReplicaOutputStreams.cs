using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>Contains the output streams for the data and checksum of a replica.</summary>
	public class ReplicaOutputStreams : IDisposable
	{
		private readonly OutputStream dataOut;

		private readonly OutputStream checksumOut;

		private readonly DataChecksum checksum;

		private readonly bool isTransientStorage;

		/// <summary>
		/// Create an object with a data output stream, a checksum output stream
		/// and a checksum.
		/// </summary>
		public ReplicaOutputStreams(OutputStream dataOut, OutputStream checksumOut, DataChecksum
			 checksum, bool isTransientStorage)
		{
			this.dataOut = dataOut;
			this.checksumOut = checksumOut;
			this.checksum = checksum;
			this.isTransientStorage = isTransientStorage;
		}

		/// <returns>the data output stream.</returns>
		public virtual OutputStream GetDataOut()
		{
			return dataOut;
		}

		/// <returns>the checksum output stream.</returns>
		public virtual OutputStream GetChecksumOut()
		{
			return checksumOut;
		}

		/// <returns>the checksum.</returns>
		public virtual DataChecksum GetChecksum()
		{
			return checksum;
		}

		/// <returns>is writing to a transient storage?</returns>
		public virtual bool IsTransientStorage()
		{
			return isTransientStorage;
		}

		public virtual void Close()
		{
			IOUtils.CloseStream(dataOut);
			IOUtils.CloseStream(checksumOut);
		}

		/// <summary>Sync the data stream if it supports it.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SyncDataOut()
		{
			if (dataOut is FileOutputStream)
			{
				((FileOutputStream)dataOut).GetChannel().Force(true);
			}
		}

		/// <summary>Sync the checksum stream if it supports it.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SyncChecksumOut()
		{
			if (checksumOut is FileOutputStream)
			{
				((FileOutputStream)checksumOut).GetChannel().Force(true);
			}
		}
	}
}
