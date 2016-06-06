using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Provides server default configuration values to clients.</summary>
	public class FsServerDefaults : IWritable
	{
		static FsServerDefaults()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.FS.FsServerDefaults), new _WritableFactory_41
				());
		}

		private sealed class _WritableFactory_41 : WritableFactory
		{
			public _WritableFactory_41()
			{
			}

			public IWritable NewInstance()
			{
				return new Org.Apache.Hadoop.FS.FsServerDefaults();
			}
		}

		private long blockSize;

		private int bytesPerChecksum;

		private int writePacketSize;

		private short replication;

		private int fileBufferSize;

		private bool encryptDataTransfer;

		private long trashInterval;

		private DataChecksum.Type checksumType;

		public FsServerDefaults()
		{
		}

		public FsServerDefaults(long blockSize, int bytesPerChecksum, int writePacketSize
			, short replication, int fileBufferSize, bool encryptDataTransfer, long trashInterval
			, DataChecksum.Type checksumType)
		{
			this.blockSize = blockSize;
			this.bytesPerChecksum = bytesPerChecksum;
			this.writePacketSize = writePacketSize;
			this.replication = replication;
			this.fileBufferSize = fileBufferSize;
			this.encryptDataTransfer = encryptDataTransfer;
			this.trashInterval = trashInterval;
			this.checksumType = checksumType;
		}

		public virtual long GetBlockSize()
		{
			return blockSize;
		}

		public virtual int GetBytesPerChecksum()
		{
			return bytesPerChecksum;
		}

		public virtual int GetWritePacketSize()
		{
			return writePacketSize;
		}

		public virtual short GetReplication()
		{
			return replication;
		}

		public virtual int GetFileBufferSize()
		{
			return fileBufferSize;
		}

		public virtual bool GetEncryptDataTransfer()
		{
			return encryptDataTransfer;
		}

		public virtual long GetTrashInterval()
		{
			return trashInterval;
		}

		public virtual DataChecksum.Type GetChecksumType()
		{
			return checksumType;
		}

		// /////////////////////////////////////////
		// Writable
		// /////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void Write(BinaryWriter writer)
		{
			@out.WriteLong(blockSize);
			@out.WriteInt(bytesPerChecksum);
			@out.WriteInt(writePacketSize);
			@out.WriteShort(replication);
			@out.WriteInt(fileBufferSize);
			WritableUtils.WriteEnum(@out, checksumType);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void ReadFields(BinaryReader reader)
		{
			blockSize = @in.ReadLong();
			bytesPerChecksum = @in.ReadInt();
			writePacketSize = @in.ReadInt();
			replication = @in.ReadShort();
			fileBufferSize = @in.ReadInt();
			checksumType = WritableUtils.ReadEnum<DataChecksum.Type>(@in);
		}
	}
}
