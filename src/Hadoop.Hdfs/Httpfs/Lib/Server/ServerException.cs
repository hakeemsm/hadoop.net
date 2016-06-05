using Org.Apache.Hadoop.Lib.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	/// <summary>
	/// Exception thrown by the
	/// <see cref="Server"/>
	/// class.
	/// </summary>
	[System.Serializable]
	public class ServerException : XException
	{
		/// <summary>
		/// Error codes use by the
		/// <see cref="Server"/>
		/// class.
		/// </summary>
		[System.Serializable]
		public sealed class ERROR : XException.ERROR
		{
			public static readonly ServerException.ERROR S01 = new ServerException.ERROR("Dir [{0}] does not exist"
				);

			public static readonly ServerException.ERROR S02 = new ServerException.ERROR("[{0}] is not a directory"
				);

			public static readonly ServerException.ERROR S03 = new ServerException.ERROR("Could not load file from classpath [{0}], {1}"
				);

			public static readonly ServerException.ERROR S04 = new ServerException.ERROR("Service [{0}] does not implement declared interface [{1}]"
				);

			public static readonly ServerException.ERROR S05 = new ServerException.ERROR("[{0}] is not a file"
				);

			public static readonly ServerException.ERROR S06 = new ServerException.ERROR("Could not load file [{0}], {1}"
				);

			public static readonly ServerException.ERROR S07 = new ServerException.ERROR("Could not instanciate service class [{0}], {1}"
				);

			public static readonly ServerException.ERROR S08 = new ServerException.ERROR("Could not load service classes, {0}"
				);

			public static readonly ServerException.ERROR S09 = new ServerException.ERROR("Could not set service [{0}] programmatically -server shutting down-, {1}"
				);

			public static readonly ServerException.ERROR S10 = new ServerException.ERROR("Service [{0}] requires service [{1}]"
				);

			public static readonly ServerException.ERROR S11 = new ServerException.ERROR("Service [{0}] exception during status change to [{1}] -server shutting down-, {2}"
				);

			public static readonly ServerException.ERROR S12 = new ServerException.ERROR("Could not start service [{0}], {1}"
				);

			public static readonly ServerException.ERROR S13 = new ServerException.ERROR("Missing system property [{0}]"
				);

			public static readonly ServerException.ERROR S14 = new ServerException.ERROR("Could not initialize server, {0}"
				);

			private string msg;

			/// <summary>Constructor for the error code enum.</summary>
			/// <param name="msg">message template.</param>
			private ERROR(string msg)
			{
				this.msg = msg;
			}

			/// <summary>Returns the message template for the error code.</summary>
			/// <returns>the message template for the error code.</returns>
			public string GetTemplate()
			{
				return ServerException.ERROR.msg;
			}
		}

		/// <summary>Constructor for sub-classes.</summary>
		/// <param name="error">error code for the XException.</param>
		/// <param name="params">
		/// parameters to use when creating the error message
		/// with the error code template.
		/// </param>
		protected internal ServerException(XException.ERROR error, params object[] @params
			)
			: base(error, @params)
		{
		}

		/// <summary>Creates an server exception using the specified error code.</summary>
		/// <remarks>
		/// Creates an server exception using the specified error code.
		/// The exception message is resolved using the error code template
		/// and the passed parameters.
		/// </remarks>
		/// <param name="error">error code for the XException.</param>
		/// <param name="params">
		/// parameters to use when creating the error message
		/// with the error code template.
		/// </param>
		public ServerException(ServerException.ERROR error, params object[] @params)
			: base(error, @params)
		{
		}
	}
}
