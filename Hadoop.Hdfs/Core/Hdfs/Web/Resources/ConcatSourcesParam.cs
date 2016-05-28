using System.Text;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>The concat source paths parameter.</summary>
	public class ConcatSourcesParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "sources";

		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		private static string Paths2String(Path[] paths)
		{
			if (paths == null || paths.Length == 0)
			{
				return string.Empty;
			}
			StringBuilder b = new StringBuilder(paths[0].ToUri().GetPath());
			for (int i = 1; i < paths.Length; i++)
			{
				b.Append(',').Append(paths[i].ToUri().GetPath());
			}
			return b.ToString();
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public ConcatSourcesParam(string str)
			: base(Domain, str)
		{
		}

		public ConcatSourcesParam(Path[] paths)
			: this(Paths2String(paths))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the absolute path.</returns>
		public string[] GetAbsolutePaths()
		{
			string[] paths = GetValue().Split(",");
			return paths;
		}
	}
}
