using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class SpillRecord
	{
		/// <summary>Backing store</summary>
		private readonly ByteBuffer buf;

		/// <summary>View of backing storage as longs</summary>
		private readonly LongBuffer entries;

		public SpillRecord(int numPartitions)
		{
			buf = ByteBuffer.Allocate(numPartitions * MapTask.MapOutputIndexRecordLength);
			entries = buf.AsLongBuffer();
		}

		/// <exception cref="System.IO.IOException"/>
		public SpillRecord(Path indexFileName, JobConf job)
			: this(indexFileName, job, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public SpillRecord(Path indexFileName, JobConf job, string expectedIndexOwner)
			: this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public SpillRecord(Path indexFileName, JobConf job, Checksum crc, string expectedIndexOwner
			)
		{
			FileSystem rfs = FileSystem.GetLocal(job).GetRaw();
			FSDataInputStream @in = SecureIOUtils.OpenFSDataInputStream(new FilePath(indexFileName
				.ToUri().GetRawPath()), expectedIndexOwner, null);
			try
			{
				long length = rfs.GetFileStatus(indexFileName).GetLen();
				int partitions = (int)length / MapTask.MapOutputIndexRecordLength;
				int size = partitions * MapTask.MapOutputIndexRecordLength;
				buf = ByteBuffer.Allocate(size);
				if (crc != null)
				{
					crc.Reset();
					CheckedInputStream chk = new CheckedInputStream(@in, crc);
					IOUtils.ReadFully(chk, ((byte[])buf.Array()), 0, size);
					if (chk.GetChecksum().GetValue() != @in.ReadLong())
					{
						throw new ChecksumException("Checksum error reading spill index: " + indexFileName
							, -1);
					}
				}
				else
				{
					IOUtils.ReadFully(@in, ((byte[])buf.Array()), 0, size);
				}
				entries = buf.AsLongBuffer();
			}
			finally
			{
				@in.Close();
			}
		}

		/// <summary>Return number of IndexRecord entries in this spill.</summary>
		public virtual int Size()
		{
			return entries.Capacity() / (MapTask.MapOutputIndexRecordLength / 8);
		}

		/// <summary>Get spill offsets for given partition.</summary>
		public virtual IndexRecord GetIndex(int partition)
		{
			int pos = partition * MapTask.MapOutputIndexRecordLength / 8;
			return new IndexRecord(entries.Get(pos), entries.Get(pos + 1), entries.Get(pos + 
				2));
		}

		/// <summary>Set spill offsets for given partition.</summary>
		public virtual void PutIndex(IndexRecord rec, int partition)
		{
			int pos = partition * MapTask.MapOutputIndexRecordLength / 8;
			entries.Put(pos, rec.startOffset);
			entries.Put(pos + 1, rec.rawLength);
			entries.Put(pos + 2, rec.partLength);
		}

		/// <summary>Write this spill record to the location provided.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteToFile(Path loc, JobConf job)
		{
			WriteToFile(loc, job, new PureJavaCrc32());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteToFile(Path loc, JobConf job, Checksum crc)
		{
			FileSystem rfs = FileSystem.GetLocal(job).GetRaw();
			CheckedOutputStream chk = null;
			FSDataOutputStream @out = rfs.Create(loc);
			try
			{
				if (crc != null)
				{
					crc.Reset();
					chk = new CheckedOutputStream(@out, crc);
					chk.Write(((byte[])buf.Array()));
					@out.WriteLong(chk.GetChecksum().GetValue());
				}
				else
				{
					@out.Write(((byte[])buf.Array()));
				}
			}
			finally
			{
				if (chk != null)
				{
					chk.Close();
				}
				else
				{
					@out.Close();
				}
			}
		}
	}
}
