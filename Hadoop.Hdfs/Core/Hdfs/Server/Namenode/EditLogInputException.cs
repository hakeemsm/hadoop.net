using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Thrown when there's a failure to read an edit log op from disk when loading
	/// edits.
	/// </summary>
	[System.Serializable]
	public class EditLogInputException : IOException
	{
		private const long serialVersionUID = 1L;

		private readonly long numEditsLoaded;

		public EditLogInputException(string message, Exception cause, long numEditsLoaded
			)
			: base(message, cause)
		{
			this.numEditsLoaded = numEditsLoaded;
		}

		public virtual long GetNumEditsLoaded()
		{
			return numEditsLoaded;
		}
	}
}
