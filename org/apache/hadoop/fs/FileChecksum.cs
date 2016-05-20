using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>An abstract class representing file checksums for files.</summary>
	public abstract class FileChecksum : org.apache.hadoop.io.Writable
	{
		/// <summary>The checksum algorithm name</summary>
		public abstract string getAlgorithmName();

		/// <summary>The length of the checksum in bytes</summary>
		public abstract int getLength();

		/// <summary>The value of the checksum in bytes</summary>
		public abstract byte[] getBytes();

		public virtual org.apache.hadoop.fs.Options.ChecksumOpt getChecksumOpt()
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
			if (other == null || !(other is org.apache.hadoop.fs.FileChecksum))
			{
				return false;
			}
			org.apache.hadoop.fs.FileChecksum that = (org.apache.hadoop.fs.FileChecksum)other;
			return this.getAlgorithmName().Equals(that.getAlgorithmName()) && java.util.Arrays
				.equals(this.getBytes(), that.getBytes());
		}

		public override int GetHashCode()
		{
			return getAlgorithmName().GetHashCode() ^ java.util.Arrays.hashCode(getBytes());
		}

		public abstract void readFields(java.io.DataInput arg1);

		public abstract void write(java.io.DataOutput arg1);
	}
}
