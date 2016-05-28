using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Wrapper for byte[] to use byte[] as key in HashMap</summary>
	public class ByteArray
	{
		private int hash = 0;

		private readonly byte[] bytes;

		public ByteArray(byte[] bytes)
		{
			// cache the hash code
			this.bytes = bytes;
		}

		public virtual byte[] GetBytes()
		{
			return bytes;
		}

		public override int GetHashCode()
		{
			if (hash == 0)
			{
				hash = Arrays.HashCode(bytes);
			}
			return hash;
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Hdfs.Util.ByteArray))
			{
				return false;
			}
			return Arrays.Equals(bytes, ((Org.Apache.Hadoop.Hdfs.Util.ByteArray)o).bytes);
		}
	}
}
