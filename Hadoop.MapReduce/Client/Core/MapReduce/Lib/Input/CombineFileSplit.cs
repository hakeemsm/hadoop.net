using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>A sub-collection of input files.</summary>
	/// <remarks>
	/// A sub-collection of input files.
	/// Unlike
	/// <see cref="FileSplit"/>
	/// , CombineFileSplit class does not represent
	/// a split of a file, but a split of input files into smaller sets.
	/// A split may contain blocks from different file but all
	/// the blocks in the same split are probably local to some rack <br />
	/// CombineFileSplit can be used to implement
	/// <see cref="Org.Apache.Hadoop.Mapreduce.RecordReader{KEYIN, VALUEIN}"/>
	/// 's,
	/// with reading one record per file.
	/// </remarks>
	/// <seealso cref="FileSplit"/>
	/// <seealso cref="CombineFileInputFormat{K, V}"></seealso>
	public class CombineFileSplit : InputSplit, Writable
	{
		private Path[] paths;

		private long[] startoffset;

		private long[] lengths;

		private string[] locations;

		private long totLength;

		/// <summary>default constructor</summary>
		public CombineFileSplit()
		{
		}

		public CombineFileSplit(Path[] files, long[] start, long[] lengths, string[] locations
			)
		{
			InitSplit(files, start, lengths, locations);
		}

		public CombineFileSplit(Path[] files, long[] lengths)
		{
			long[] startoffset = new long[files.Length];
			for (int i = 0; i < startoffset.Length; i++)
			{
				startoffset[i] = 0;
			}
			string[] locations = new string[files.Length];
			for (int i_1 = 0; i_1 < locations.Length; i_1++)
			{
				locations[i_1] = string.Empty;
			}
			InitSplit(files, startoffset, lengths, locations);
		}

		private void InitSplit(Path[] files, long[] start, long[] lengths, string[] locations
			)
		{
			this.startoffset = start;
			this.lengths = lengths;
			this.paths = files;
			this.totLength = 0;
			this.locations = locations;
			foreach (long length in lengths)
			{
				totLength += length;
			}
		}

		/// <summary>Copy constructor</summary>
		/// <exception cref="System.IO.IOException"/>
		public CombineFileSplit(Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileSplit old
			)
			: this(old.GetPaths(), old.GetStartOffsets(), old.GetLengths(), old.GetLocations(
				))
		{
		}

		public override long GetLength()
		{
			return totLength;
		}

		/// <summary>Returns an array containing the start offsets of the files in the split</summary>
		public virtual long[] GetStartOffsets()
		{
			return startoffset;
		}

		/// <summary>Returns an array containing the lengths of the files in the split</summary>
		public virtual long[] GetLengths()
		{
			return lengths;
		}

		/// <summary>Returns the start offset of the i<sup>th</sup> Path</summary>
		public virtual long GetOffset(int i)
		{
			return startoffset[i];
		}

		/// <summary>Returns the length of the i<sup>th</sup> Path</summary>
		public virtual long GetLength(int i)
		{
			return lengths[i];
		}

		/// <summary>Returns the number of Paths in the split</summary>
		public virtual int GetNumPaths()
		{
			return paths.Length;
		}

		/// <summary>Returns the i<sup>th</sup> Path</summary>
		public virtual Path GetPath(int i)
		{
			return paths[i];
		}

		/// <summary>Returns all the Paths in the split</summary>
		public virtual Path[] GetPaths()
		{
			return paths;
		}

		/// <summary>Returns all the Paths where this input-split resides</summary>
		/// <exception cref="System.IO.IOException"/>
		public override string[] GetLocations()
		{
			return locations;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			totLength = @in.ReadLong();
			int arrLength = @in.ReadInt();
			lengths = new long[arrLength];
			for (int i = 0; i < arrLength; i++)
			{
				lengths[i] = @in.ReadLong();
			}
			int filesLength = @in.ReadInt();
			paths = new Path[filesLength];
			for (int i_1 = 0; i_1 < filesLength; i_1++)
			{
				paths[i_1] = new Path(Text.ReadString(@in));
			}
			arrLength = @in.ReadInt();
			startoffset = new long[arrLength];
			for (int i_2 = 0; i_2 < arrLength; i_2++)
			{
				startoffset[i_2] = @in.ReadLong();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteLong(totLength);
			@out.WriteInt(lengths.Length);
			foreach (long length in lengths)
			{
				@out.WriteLong(length);
			}
			@out.WriteInt(paths.Length);
			foreach (Path p in paths)
			{
				Text.WriteString(@out, p.ToString());
			}
			@out.WriteInt(startoffset.Length);
			foreach (long length_1 in startoffset)
			{
				@out.WriteLong(length_1);
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < paths.Length; i++)
			{
				if (i == 0)
				{
					sb.Append("Paths:");
				}
				sb.Append(paths[i].ToUri().GetPath() + ":" + startoffset[i] + "+" + lengths[i]);
				if (i < paths.Length - 1)
				{
					sb.Append(",");
				}
			}
			if (locations != null)
			{
				string locs = string.Empty;
				StringBuilder locsb = new StringBuilder();
				for (int i_1 = 0; i_1 < locations.Length; i_1++)
				{
					locsb.Append(locations[i_1] + ":");
				}
				locs = locsb.ToString();
				sb.Append(" Locations:" + locs + "; ");
			}
			return sb.ToString();
		}
	}
}
