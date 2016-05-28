using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Contains a list of paths corresponding to corrupt files and a cookie
	/// used for iterative calls to NameNode.listCorruptFileBlocks.
	/// </summary>
	public class CorruptFileBlocks
	{
		private const int Prime = 16777619;

		private readonly string[] files;

		private readonly string cookie;

		public CorruptFileBlocks()
			: this(new string[0], string.Empty)
		{
		}

		public CorruptFileBlocks(string[] files, string cookie)
		{
			// used for hashCode
			this.files = files;
			this.cookie = cookie;
		}

		public virtual string[] GetFiles()
		{
			return files;
		}

		public virtual string GetCookie()
		{
			return cookie;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (!(obj is Org.Apache.Hadoop.Hdfs.Protocol.CorruptFileBlocks))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.CorruptFileBlocks other = (Org.Apache.Hadoop.Hdfs.Protocol.CorruptFileBlocks
				)obj;
			return cookie.Equals(other.cookie) && Arrays.Equals(files, other.files);
		}

		public override int GetHashCode()
		{
			int result = cookie.GetHashCode();
			foreach (string file in files)
			{
				result = Prime * result + file.GetHashCode();
			}
			return result;
		}
	}
}
