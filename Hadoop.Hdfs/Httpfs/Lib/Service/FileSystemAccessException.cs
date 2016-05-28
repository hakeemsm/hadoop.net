using Org.Apache.Hadoop.Lib.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service
{
	[System.Serializable]
	public class FileSystemAccessException : XException
	{
		[System.Serializable]
		public sealed class ERROR : XException.ERROR
		{
			public static readonly FileSystemAccessException.ERROR H01 = new FileSystemAccessException.ERROR
				("Service property [{0}] not defined");

			public static readonly FileSystemAccessException.ERROR H02 = new FileSystemAccessException.ERROR
				("Kerberos initialization failed, {0}");

			public static readonly FileSystemAccessException.ERROR H03 = new FileSystemAccessException.ERROR
				("FileSystemExecutor error, {0}");

			public static readonly FileSystemAccessException.ERROR H04 = new FileSystemAccessException.ERROR
				("Invalid configuration, it has not be created by the FileSystemAccessService");

			public static readonly FileSystemAccessException.ERROR H05 = new FileSystemAccessException.ERROR
				("[{0}] validation failed, {1}");

			public static readonly FileSystemAccessException.ERROR H06 = new FileSystemAccessException.ERROR
				("Property [{0}] not defined in configuration object");

			public static readonly FileSystemAccessException.ERROR H07 = new FileSystemAccessException.ERROR
				("[{0}] not healthy, {1}");

			public static readonly FileSystemAccessException.ERROR H08 = new FileSystemAccessException.ERROR
				("{0}");

			public static readonly FileSystemAccessException.ERROR H09 = new FileSystemAccessException.ERROR
				("Invalid FileSystemAccess security mode [{0}]");

			public static readonly FileSystemAccessException.ERROR H10 = new FileSystemAccessException.ERROR
				("Hadoop config directory not found [{0}]");

			public static readonly FileSystemAccessException.ERROR H11 = new FileSystemAccessException.ERROR
				("Could not load Hadoop config files, {0}");

			private string template;

			internal ERROR(string template)
			{
				this.template = template;
			}

			public string GetTemplate()
			{
				return FileSystemAccessException.ERROR.template;
			}
		}

		public FileSystemAccessException(FileSystemAccessException.ERROR error, params object
			[] @params)
			: base(error, @params)
		{
		}
	}
}
