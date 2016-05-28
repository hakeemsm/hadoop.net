using System;
using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A section of an input file.</summary>
	/// <remarks>
	/// A section of an input file.  Returned by
	/// <see cref="InputFormat{K, V}.GetSplits(JobConf, int)"/>
	/// and passed to
	/// <see cref="InputFormat{K, V}.GetRecordReader(InputSplit, JobConf, Reporter)"/>
	/// .
	/// </remarks>
	public class FileSplit : InputSplit, InputSplitWithLocationInfo
	{
		internal Org.Apache.Hadoop.Mapreduce.Lib.Input.FileSplit fs;

		protected internal FileSplit()
		{
			fs = new Org.Apache.Hadoop.Mapreduce.Lib.Input.FileSplit();
		}

		/// <summary>Constructs a split.</summary>
		/// <param name="file">the file name</param>
		/// <param name="start">the position of the first byte in the file to process</param>
		/// <param name="length">the number of bytes in the file to process</param>
		[System.ObsoleteAttribute]
		public FileSplit(Path file, long start, long length, JobConf conf)
			: this(file, start, length, (string[])null)
		{
		}

		/// <summary>Constructs a split with host information</summary>
		/// <param name="file">the file name</param>
		/// <param name="start">the position of the first byte in the file to process</param>
		/// <param name="length">the number of bytes in the file to process</param>
		/// <param name="hosts">the list of hosts containing the block, possibly null</param>
		public FileSplit(Path file, long start, long length, string[] hosts)
		{
			fs = new Org.Apache.Hadoop.Mapreduce.Lib.Input.FileSplit(file, start, length, hosts
				);
		}

		/// <summary>Constructs a split with host information</summary>
		/// <param name="file">the file name</param>
		/// <param name="start">the position of the first byte in the file to process</param>
		/// <param name="length">the number of bytes in the file to process</param>
		/// <param name="hosts">the list of hosts containing the block, possibly null</param>
		/// <param name="inMemoryHosts">the list of hosts containing the block in memory</param>
		public FileSplit(Path file, long start, long length, string[] hosts, string[] inMemoryHosts
			)
		{
			fs = new Org.Apache.Hadoop.Mapreduce.Lib.Input.FileSplit(file, start, length, hosts
				, inMemoryHosts);
		}

		public FileSplit(Org.Apache.Hadoop.Mapreduce.Lib.Input.FileSplit fs)
		{
			this.fs = fs;
		}

		/// <summary>The file containing this split's data.</summary>
		public virtual Path GetPath()
		{
			return fs.GetPath();
		}

		/// <summary>The position of the first byte in the file to process.</summary>
		public virtual long GetStart()
		{
			return fs.GetStart();
		}

		/// <summary>The number of bytes in the file to process.</summary>
		public override long GetLength()
		{
			return fs.GetLength();
		}

		public override string ToString()
		{
			return fs.ToString();
		}

		////////////////////////////////////////////
		// Writable methods
		////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			fs.Write(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			fs.ReadFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override string[] GetLocations()
		{
			return fs.GetLocations();
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceStability.Evolving]
		public override SplitLocationInfo[] GetLocationInfo()
		{
			return fs.GetLocationInfo();
		}
	}
}
