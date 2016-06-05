using System.IO;
using System.Text;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// A Block is a Hadoop FS primitive, identified by a
	/// long.
	/// </summary>
	public class Block : Writable, Comparable<Org.Apache.Hadoop.Hdfs.Protocol.Block>
	{
		public const string BlockFilePrefix = "blk_";

		public const string MetadataExtension = ".meta";

		static Block()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Hdfs.Protocol.Block), new _WritableFactory_42
				());
		}

		private sealed class _WritableFactory_42 : WritableFactory
		{
			public _WritableFactory_42()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Hdfs.Protocol.Block();
			}
		}

		public static readonly Sharpen.Pattern blockFilePattern = Sharpen.Pattern.Compile
			(BlockFilePrefix + "(-??\\d++)$");

		public static readonly Sharpen.Pattern metaFilePattern = Sharpen.Pattern.Compile(
			BlockFilePrefix + "(-??\\d++)_(\\d++)\\" + MetadataExtension + "$");

		public static readonly Sharpen.Pattern metaOrBlockFilePattern = Sharpen.Pattern.Compile
			(BlockFilePrefix + "(-??\\d++)(_(\\d++)\\" + MetadataExtension + ")?$");

		public static bool IsBlockFilename(FilePath f)
		{
			string name = f.GetName();
			return blockFilePattern.Matcher(name).Matches();
		}

		public static long Filename2id(string name)
		{
			Matcher m = blockFilePattern.Matcher(name);
			return m.Matches() ? long.Parse(m.Group(1)) : 0;
		}

		public static bool IsMetaFilename(string name)
		{
			return metaFilePattern.Matcher(name).Matches();
		}

		public static FilePath MetaToBlockFile(FilePath metaFile)
		{
			return new FilePath(metaFile.GetParent(), Sharpen.Runtime.Substring(metaFile.GetName
				(), 0, metaFile.GetName().LastIndexOf('_')));
		}

		/// <summary>Get generation stamp from the name of the metafile name</summary>
		public static long GetGenerationStamp(string metaFile)
		{
			Matcher m = metaFilePattern.Matcher(metaFile);
			return m.Matches() ? long.Parse(m.Group(2)) : GenerationStamp.GrandfatherGenerationStamp;
		}

		/// <summary>Get the blockId from the name of the meta or block file</summary>
		public static long GetBlockId(string metaOrBlockFile)
		{
			Matcher m = metaOrBlockFilePattern.Matcher(metaOrBlockFile);
			return m.Matches() ? long.Parse(m.Group(1)) : 0;
		}

		private long blockId;

		private long numBytes;

		private long generationStamp;

		public Block()
			: this(0, 0, 0)
		{
		}

		public Block(long blkid, long len, long generationStamp)
		{
			Set(blkid, len, generationStamp);
		}

		public Block(long blkid)
			: this(blkid, 0, GenerationStamp.GrandfatherGenerationStamp)
		{
		}

		public Block(Org.Apache.Hadoop.Hdfs.Protocol.Block blk)
			: this(blk.blockId, blk.numBytes, blk.generationStamp)
		{
		}

		/// <summary>Find the blockid from the given filename</summary>
		public Block(FilePath f, long len, long genstamp)
			: this(Filename2id(f.GetName()), len, genstamp)
		{
		}

		public virtual void Set(long blkid, long len, long genStamp)
		{
			this.blockId = blkid;
			this.numBytes = len;
			this.generationStamp = genStamp;
		}

		public virtual long GetBlockId()
		{
			return blockId;
		}

		public virtual void SetBlockId(long bid)
		{
			blockId = bid;
		}

		public virtual string GetBlockName()
		{
			return BlockFilePrefix + blockId.ToString();
		}

		public virtual long GetNumBytes()
		{
			return numBytes;
		}

		public virtual void SetNumBytes(long len)
		{
			this.numBytes = len;
		}

		public virtual long GetGenerationStamp()
		{
			return generationStamp;
		}

		public virtual void SetGenerationStamp(long stamp)
		{
			generationStamp = stamp;
		}

		public override string ToString()
		{
			return GetBlockName() + "_" + GetGenerationStamp();
		}

		public virtual void AppendStringTo(StringBuilder sb)
		{
			sb.Append(BlockFilePrefix).Append(blockId).Append("_").Append(GetGenerationStamp(
				));
		}

		/////////////////////////////////////
		// Writable
		/////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			// Writable
			WriteHelper(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			// Writable
			ReadHelper(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		internal void WriteHelper(DataOutput @out)
		{
			@out.WriteLong(blockId);
			@out.WriteLong(numBytes);
			@out.WriteLong(generationStamp);
		}

		/// <exception cref="System.IO.IOException"/>
		internal void ReadHelper(DataInput @in)
		{
			this.blockId = @in.ReadLong();
			this.numBytes = @in.ReadLong();
			this.generationStamp = @in.ReadLong();
			if (numBytes < 0)
			{
				throw new IOException("Unexpected block size: " + numBytes);
			}
		}

		// write only the identifier part of the block
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteId(DataOutput @out)
		{
			@out.WriteLong(blockId);
			@out.WriteLong(generationStamp);
		}

		// Read only the identifier part of the block
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadId(DataInput @in)
		{
			this.blockId = @in.ReadLong();
			this.generationStamp = @in.ReadLong();
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Hdfs.Protocol.Block b)
		{
			// Comparable
			return blockId < b.blockId ? -1 : blockId > b.blockId ? 1 : 0;
		}

		public override bool Equals(object o)
		{
			// Object
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Hdfs.Protocol.Block))
			{
				return false;
			}
			return CompareTo((Org.Apache.Hadoop.Hdfs.Protocol.Block)o) == 0;
		}

		/// <returns>
		/// true if the two blocks have the same block ID and the same
		/// generation stamp, or if both blocks are null.
		/// </returns>
		public static bool MatchingIdAndGenStamp(Org.Apache.Hadoop.Hdfs.Protocol.Block a, 
			Org.Apache.Hadoop.Hdfs.Protocol.Block b)
		{
			if (a == b)
			{
				return true;
			}
			// same block, or both null
			if (a == null || b == null)
			{
				return false;
			}
			// only one null
			return a.blockId == b.blockId && a.generationStamp == b.generationStamp;
		}

		public override int GetHashCode()
		{
			// Object
			//GenerationStamp is IRRELEVANT and should not be used here
			return (int)(blockId ^ ((long)(((ulong)blockId) >> 32)));
		}
	}
}
