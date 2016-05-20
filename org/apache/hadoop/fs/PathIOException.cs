using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Exceptions based on standard posix/linux style exceptions for path related
	/// errors.
	/// </summary>
	/// <remarks>
	/// Exceptions based on standard posix/linux style exceptions for path related
	/// errors. Returns an exception with the format "path: standard error string".
	/// This exception corresponds to Error Input/ouput(EIO)
	/// </remarks>
	[System.Serializable]
	public class PathIOException : System.IO.IOException
	{
		internal const long serialVersionUID = 0L;

		private const string EIO = "Input/output error";

		private string operation;

		private string path;

		private string targetPath;

		/// <summary>Constructor a generic I/O error exception</summary>
		/// <param name="path">for the exception</param>
		public PathIOException(string path)
			: this(path, EIO)
		{
		}

		/// <summary>Appends the text of a Throwable to the default error message</summary>
		/// <param name="path">for the exception</param>
		/// <param name="cause">a throwable to extract the error message</param>
		public PathIOException(string path, System.Exception cause)
			: this(path, EIO, cause)
		{
		}

		/// <summary>Avoid using this method.</summary>
		/// <remarks>
		/// Avoid using this method.  Use a subclass of PathIOException if
		/// possible.
		/// </remarks>
		/// <param name="path">for the exception</param>
		/// <param name="error">custom string to use an the error text</param>
		public PathIOException(string path, string error)
			: base(error)
		{
			// NOTE: this really should be a Path, but a Path is buggy and won't
			// return the exact string used to construct the path, and it mangles
			// uris with no authority
			this.path = path;
		}

		protected internal PathIOException(string path, string error, System.Exception cause
			)
			: base(error, cause)
		{
			this.path = path;
		}

		/// <summary>
		/// Format:
		/// cmd: {operation} `path' {to `target'}: error string
		/// </summary>
		public override string Message
		{
			get
			{
				java.lang.StringBuilder message = new java.lang.StringBuilder();
				if (operation != null)
				{
					message.Append(operation + " ");
				}
				message.Append(formatPath(path));
				if (targetPath != null)
				{
					message.Append(" to " + formatPath(targetPath));
				}
				message.Append(": " + base.Message);
				if (InnerException != null)
				{
					message.Append(": " + InnerException.Message);
				}
				return message.ToString();
			}
		}

		/// <returns>Path that generated the exception</returns>
		public virtual org.apache.hadoop.fs.Path getPath()
		{
			return new org.apache.hadoop.fs.Path(path);
		}

		/// <returns>Path if the operation involved copying or moving, else null</returns>
		public virtual org.apache.hadoop.fs.Path getTargetPath()
		{
			return (targetPath != null) ? new org.apache.hadoop.fs.Path(targetPath) : null;
		}

		/// <summary>Optional operation that will preface the path</summary>
		/// <param name="operation">a string</param>
		public virtual void setOperation(string operation)
		{
			this.operation = operation;
		}

		/// <summary>Optional path if the exception involved two paths, ex.</summary>
		/// <remarks>Optional path if the exception involved two paths, ex. a copy operation</remarks>
		/// <param name="targetPath">the of the operation</param>
		public virtual void setTargetPath(string targetPath)
		{
			this.targetPath = targetPath;
		}

		private string formatPath(string path)
		{
			return "`" + path + "'";
		}
	}
}
