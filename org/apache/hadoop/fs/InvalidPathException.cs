using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Path string is invalid either because it has invalid characters or due to
	/// other file system specific reasons.
	/// </summary>
	[System.Serializable]
	public class InvalidPathException : org.apache.hadoop.HadoopIllegalArgumentException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="path">invalid path.</param>
		public InvalidPathException(string path)
			: base("Invalid path name " + path)
		{
		}

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="path">invalid path.</param>
		/// <param name="reason">Reason <code>path</code> is invalid</param>
		public InvalidPathException(string path, string reason)
			: base("Invalid path " + path + (reason == null ? string.Empty : ". (" + reason +
				 ")"))
		{
		}
	}
}
