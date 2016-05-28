using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Exception used to signify file reading failures where headers are bad or an
	/// unexpected EOF occurs when it should not.
	/// </summary>
	[System.Serializable]
	internal class BadFileException : IOException
	{
		private const long serialVersionUID = 463201983951298129L;

		internal BadFileException(string msg)
			: base(msg)
		{
		}

		internal BadFileException(string msg, Exception e)
			: base(msg, e)
		{
		}
	}
}
