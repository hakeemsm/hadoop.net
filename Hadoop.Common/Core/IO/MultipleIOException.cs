using System.Collections.Generic;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// Encapsulate a list of
	/// <see cref="System.IO.IOException"/>
	/// into an
	/// <see cref="System.IO.IOException"/>
	/// 
	/// </summary>
	[System.Serializable]
	public class MultipleIOException : IOException
	{
		/// <summary>
		/// Require by
		/// <see cref="System.IO.Serializable"/>
		/// 
		/// </summary>
		private const long serialVersionUID = 1L;

		private readonly IList<IOException> exceptions;

		/// <summary>
		/// Constructor is private, use
		/// <see cref="CreateIOException(System.Collections.Generic.IList{E})"/>
		/// .
		/// </summary>
		private MultipleIOException(IList<IOException> exceptions)
			: base(exceptions.Count + " exceptions " + exceptions)
		{
			this.exceptions = exceptions;
		}

		/// <returns>the underlying exceptions</returns>
		public virtual IList<IOException> GetExceptions()
		{
			return exceptions;
		}

		/// <summary>
		/// A convenient method to create an
		/// <see cref="System.IO.IOException"/>
		/// .
		/// </summary>
		public static IOException CreateIOException(IList<IOException> exceptions)
		{
			if (exceptions == null || exceptions.IsEmpty())
			{
				return null;
			}
			if (exceptions.Count == 1)
			{
				return exceptions[0];
			}
			return new Org.Apache.Hadoop.IO.MultipleIOException(exceptions);
		}
	}
}
