using Sharpen;

namespace org.apache.hadoop
{
	/// <summary>Indicates that a method has been passed illegal or invalid argument.</summary>
	/// <remarks>
	/// Indicates that a method has been passed illegal or invalid argument. This
	/// exception is thrown instead of IllegalArgumentException to differentiate the
	/// exception thrown in Hadoop implementation from the one thrown in JDK.
	/// </remarks>
	[System.Serializable]
	public class HadoopIllegalArgumentException : System.ArgumentException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="message">detailed message.</param>
		public HadoopIllegalArgumentException(string message)
			: base(message)
		{
		}
	}
}
