using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class ConfInfo
	{
		protected internal string path;

		protected internal AList<ConfEntryInfo> property;

		public ConfInfo()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public ConfInfo(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			this.property = new AList<ConfEntryInfo>();
			Configuration jobConf = job.LoadConfFile();
			this.path = job.GetConfFile().ToString();
			foreach (KeyValuePair<string, string> entry in jobConf)
			{
				this.property.AddItem(new ConfEntryInfo(entry.Key, entry.Value, jobConf.GetPropertySources
					(entry.Key)));
			}
		}

		public virtual AList<ConfEntryInfo> GetProperties()
		{
			return this.property;
		}

		public virtual string GetPath()
		{
			return this.path;
		}
	}
}
