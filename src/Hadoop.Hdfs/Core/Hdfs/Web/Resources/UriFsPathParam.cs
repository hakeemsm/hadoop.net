using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>The FileSystem path parameter.</summary>
	public class UriFsPathParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "path";

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public UriFsPathParam(string str)
			: base(Domain, str)
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the absolute path.</returns>
		public string GetAbsolutePath()
		{
			string path = GetValue();
			//The first / has been stripped out.
			return path == null ? null : "/" + path;
		}
	}
}
