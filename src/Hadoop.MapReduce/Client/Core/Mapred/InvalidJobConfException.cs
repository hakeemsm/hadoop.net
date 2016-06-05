using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This exception is thrown when jobconf misses some mendatory attributes
	/// or value of some attributes is invalid.
	/// </summary>
	[System.Serializable]
	public class InvalidJobConfException : IOException
	{
		private const long serialVersionUID = 1L;

		public InvalidJobConfException()
			: base()
		{
		}

		public InvalidJobConfException(string msg)
			: base(msg)
		{
		}

		public InvalidJobConfException(string msg, Exception t)
			: base(msg, t)
		{
		}

		public InvalidJobConfException(Exception t)
			: base(t)
		{
		}
	}
}
