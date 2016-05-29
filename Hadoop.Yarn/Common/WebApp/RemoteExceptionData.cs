using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>
	/// Contains the exception information from an exception thrown
	/// by the web service REST API's.
	/// </summary>
	/// <remarks>
	/// Contains the exception information from an exception thrown
	/// by the web service REST API's.
	/// Fields include:
	/// exception - exception type
	/// javaClassName - java class name of the exception
	/// message - a detailed message explaining the exception
	/// </remarks>
	public class RemoteExceptionData
	{
		private string exception;

		private string message;

		private string javaClassName;

		public RemoteExceptionData()
		{
		}

		public RemoteExceptionData(string excep, string message, string className)
		{
			this.exception = excep;
			this.message = message;
			this.javaClassName = className;
		}

		public virtual string GetException()
		{
			return exception;
		}

		public virtual string GetMessage()
		{
			return message;
		}

		public virtual string GetJavaClassName()
		{
			return javaClassName;
		}
	}
}
