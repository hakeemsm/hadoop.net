using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// Encapsulate a list of
	/// <see cref="System.IO.IOException"/>
	/// into an
	/// <see cref="System.IO.IOException"/>
	/// 
	/// </summary>
	[System.Serializable]
	public class MultipleIOException : System.IO.IOException
	{
		/// <summary>
		/// Require by
		/// <see cref="java.io.Serializable"/>
		/// 
		/// </summary>
		private const long serialVersionUID = 1L;

		private readonly System.Collections.Generic.IList<System.IO.IOException> exceptions;

		/// <summary>
		/// Constructor is private, use
		/// <see cref="createIOException(System.Collections.Generic.IList{E})"/>
		/// .
		/// </summary>
		private MultipleIOException(System.Collections.Generic.IList<System.IO.IOException
			> exceptions)
			: base(exceptions.Count + " exceptions " + exceptions)
		{
			this.exceptions = exceptions;
		}

		/// <returns>the underlying exceptions</returns>
		public virtual System.Collections.Generic.IList<System.IO.IOException> getExceptions
			()
		{
			return exceptions;
		}

		/// <summary>
		/// A convenient method to create an
		/// <see cref="System.IO.IOException"/>
		/// .
		/// </summary>
		public static System.IO.IOException createIOException(System.Collections.Generic.IList
			<System.IO.IOException> exceptions)
		{
			if (exceptions == null || exceptions.isEmpty())
			{
				return null;
			}
			if (exceptions.Count == 1)
			{
				return exceptions[0];
			}
			return new org.apache.hadoop.io.MultipleIOException(exceptions);
		}
	}
}
