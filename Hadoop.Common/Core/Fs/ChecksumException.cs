using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Thrown for checksum errors.</summary>
	[System.Serializable]
	public class ChecksumException : IOException
	{
		private const long serialVersionUID = 1L;

		private long pos;

		public ChecksumException(string description, long pos)
			: base(description)
		{
			this.pos = pos;
		}

		public virtual long GetPos()
		{
			return pos;
		}
	}
}
