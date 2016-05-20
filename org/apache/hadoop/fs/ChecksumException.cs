using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Thrown for checksum errors.</summary>
	[System.Serializable]
	public class ChecksumException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		private long pos;

		public ChecksumException(string description, long pos)
			: base(description)
		{
			this.pos = pos;
		}

		public virtual long getPos()
		{
			return pos;
		}
	}
}
