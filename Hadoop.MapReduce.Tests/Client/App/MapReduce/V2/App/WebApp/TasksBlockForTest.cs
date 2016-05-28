using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Class TasksBlockForTest overrides some methods for test</summary>
	public class TasksBlockForTest : TasksBlock
	{
		private readonly IDictionary<string, string> @params = new Dictionary<string, string
			>();

		public TasksBlockForTest(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app)
			: base(app)
		{
		}

		public virtual void AddParameter(string name, string value)
		{
			@params[name] = value;
		}

		public override string $(string key, string defaultValue)
		{
			string value = @params[key];
			return value == null ? defaultValue : value;
		}

		public override string Url(params string[] parts)
		{
			string result = "url://";
			foreach (string @string in parts)
			{
				result += @string + ":";
			}
			return result;
		}
	}
}
