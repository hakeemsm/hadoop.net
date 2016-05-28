using Org.Apache.Hadoop.Lib.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	/// <summary>
	/// Exception thrown by
	/// <see cref="Service"/>
	/// implementations.
	/// </summary>
	[System.Serializable]
	public class ServiceException : ServerException
	{
		/// <summary>Creates an service exception using the specified error code.</summary>
		/// <remarks>
		/// Creates an service exception using the specified error code.
		/// The exception message is resolved using the error code template
		/// and the passed parameters.
		/// </remarks>
		/// <param name="error">error code for the XException.</param>
		/// <param name="params">
		/// parameters to use when creating the error message
		/// with the error code template.
		/// </param>
		public ServiceException(XException.ERROR error, params object[] @params)
			: base(error, @params)
		{
		}
	}
}
