using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>
	/// The exception is thrown when external version does not match
	/// current version of the application.
	/// </summary>
	[System.Serializable]
	public class IncorrectVersionException : IOException
	{
		private const long serialVersionUID = 1L;

		public IncorrectVersionException(string message)
			: base(message)
		{
		}

		public IncorrectVersionException(string minimumVersion, string reportedVersion, string
			 remoteDaemon, string thisDaemon)
			: this("The reported " + remoteDaemon + " version is too low to communicate" + " with this "
				 + thisDaemon + ". " + remoteDaemon + " version: '" + reportedVersion + "' Minimum "
				 + remoteDaemon + " version: '" + minimumVersion + "'")
		{
		}

		public IncorrectVersionException(int currentLayoutVersion, int versionReported, string
			 ofWhat)
			: this(versionReported, ofWhat, currentLayoutVersion)
		{
		}

		public IncorrectVersionException(int versionReported, string ofWhat, int versionExpected
			)
			: this("Unexpected version " + (ofWhat == null ? string.Empty : "of " + ofWhat) +
				 ". Reported: " + versionReported + ". Expecting = " + versionExpected + ".")
		{
		}
	}
}
