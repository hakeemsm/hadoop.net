using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Provides server default configuration values to clients.</summary>
	public class FsServerDefaults : org.apache.hadoop.io.Writable
	{
		static FsServerDefaults()
		{
			// register a ctor
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.FsServerDefaults)), new _WritableFactory_41());
		}

		private sealed class _WritableFactory_41 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_41()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.fs.FsServerDefaults();
			}
		}

		private long blockSize;

		private int bytesPerChecksum;

		private int writePacketSize;

		private short replication;

		private int fileBufferSize;

		private bool encryptDataTransfer;

		private long trashInterval;

		private org.apache.hadoop.util.DataChecksum.Type checksumType;

		public FsServerDefaults()
		{
		}

		public FsServerDefaults(long blockSize, int bytesPerChecksum, int writePacketSize
			, short replication, int fileBufferSize, bool encryptDataTransfer, long trashInterval
			, org.apache.hadoop.util.DataChecksum.Type checksumType)
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

		public virtual long getBlockSize()
		{
			return blockSize;
		}

		public virtual int getBytesPerChecksum()
		{
			return bytesPerChecksum;
		}

		public virtual int getWritePacketSize()
		{
			return writePacketSize;
		}

		public virtual short getReplication()
		{
			return replication;
		}

		public virtual int getFileBufferSize()
		{
			return fileBufferSize;
		}

		public virtual bool getEncryptDataTransfer()
		{
			return encryptDataTransfer;
		}

		public virtual long getTrashInterval()
		{
			return trashInterval;
		}

		public virtual org.apache.hadoop.util.DataChecksum.Type getChecksumType()
		{
			return checksumType;
		}

		// /////////////////////////////////////////
		// Writable
		// /////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeLong(blockSize);
			@out.writeInt(bytesPerChecksum);
			@out.writeInt(writePacketSize);
			@out.writeShort(replication);
			@out.writeInt(fileBufferSize);
			org.apache.hadoop.io.WritableUtils.writeEnum(@out, checksumType);
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void readFields(java.io.DataInput @in)
		{
			blockSize = @in.readLong();
			bytesPerChecksum = @in.readInt();
			writePacketSize = @in.readInt();
			replication = @in.readShort();
			fileBufferSize = @in.readInt();
			checksumType = org.apache.hadoop.io.WritableUtils.readEnum<org.apache.hadoop.util.DataChecksum.Type
				>(@in);
		}
	}
}
