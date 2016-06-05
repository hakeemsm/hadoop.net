using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>An abstract class representing file checksums for files.</summary>
	public abstract class FileChecksum : IWritable
	{
		/// <summary>The checksum algorithm name</summary>
		public abstract string GetAlgorithmName();

		/// <summary>The length of the checksum in bytes</summary>
		public abstract int GetLength();

		/// <summary>The value of the checksum in bytes</summary>
		public abstract byte[] GetBytes();

		public virtual Options.ChecksumOpt GetChecksumOpt()
		{
			return null;
		}

		/// <summary>Return true if both the algorithms and the values are the same.</summary>
		public override bool Equals(object other)
		{
			if (other == this)
			{
				return true;
			}
			if (other == null || !(other is FileChecksum))
			{
				return false;
			}
			FileChecksum that = (FileChecksum)other;
			return this.GetAlgorithmName().Equals(that.GetAlgorithmName()) && Arrays.Equals(this
				.GetBytes(), that.GetBytes());
		}

		public override int GetHashCode()
		{
			return GetAlgorithmName().GetHashCode() ^ Arrays.HashCode(GetBytes());
		}

		public abstract void ReadFields(BinaryReader arg1);

		public abstract void Write(BinaryWriter arg1);
	}
}
