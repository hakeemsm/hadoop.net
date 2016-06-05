using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>A section of an input file.</summary>
	/// <remarks>
	/// A section of an input file.  Returned by
	/// <see cref="InputFormat#getSplits(JobContext)"/>
	/// and passed to
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}.CreateRecordReader(Org.Apache.Hadoop.Mapreduce.InputSplit, Org.Apache.Hadoop.Mapreduce.TaskAttemptContext)
	/// 	"/>
	/// .
	/// </remarks>
	public class FileSplit : InputSplit, Writable
	{
		private Path file;

		private long start;

		private long length;

		private string[] hosts;

		private SplitLocationInfo[] hostInfos;

		public FileSplit()
		{
		}

		/// <summary>Constructs a split with host information</summary>
		/// <param name="file">the file name</param>
		/// <param name="start">the position of the first byte in the file to process</param>
		/// <param name="length">the number of bytes in the file to process</param>
		/// <param name="hosts">the list of hosts containing the block, possibly null</param>
		public FileSplit(Path file, long start, long length, string[] hosts)
		{
			this.file = file;
			this.start = start;
			this.length = length;
			this.hosts = hosts;
		}

		/// <summary>Constructs a split with host and cached-blocks information</summary>
		/// <param name="file">the file name</param>
		/// <param name="start">the position of the first byte in the file to process</param>
		/// <param name="length">the number of bytes in the file to process</param>
		/// <param name="hosts">the list of hosts containing the block</param>
		/// <param name="inMemoryHosts">the list of hosts containing the block in memory</param>
		public FileSplit(Path file, long start, long length, string[] hosts, string[] inMemoryHosts
			)
			: this(file, start, length, hosts)
		{
			hostInfos = new SplitLocationInfo[hosts.Length];
			for (int i = 0; i < hosts.Length; i++)
			{
				// because N will be tiny, scanning is probably faster than a HashSet
				bool inMemory = false;
				foreach (string inMemoryHost in inMemoryHosts)
				{
					if (inMemoryHost.Equals(hosts[i]))
					{
						inMemory = true;
						break;
					}
				}
				hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
			}
		}

		/// <summary>The file containing this split's data.</summary>
		public virtual Path GetPath()
		{
			return file;
		}

		/// <summary>The position of the first byte in the file to process.</summary>
		public virtual long GetStart()
		{
			return start;
		}

		/// <summary>The number of bytes in the file to process.</summary>
		public override long GetLength()
		{
			return length;
		}

		public override string ToString()
		{
			return file + ":" + start + "+" + length;
		}

		////////////////////////////////////////////
		// Writable methods
		////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, file.ToString());
			@out.WriteLong(start);
			@out.WriteLong(length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			file = new Path(Text.ReadString(@in));
			start = @in.ReadLong();
			length = @in.ReadLong();
			hosts = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override string[] GetLocations()
		{
			if (this.hosts == null)
			{
				return new string[] {  };
			}
			else
			{
				return this.hosts;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceStability.Evolving]
		public override SplitLocationInfo[] GetLocationInfo()
		{
			return hostInfos;
		}
	}
}
