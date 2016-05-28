using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Render the configuration for this job.</summary>
	public class ConfBlock : HtmlBlock
	{
		internal readonly AppContext appContext;

		[Com.Google.Inject.Inject]
		internal ConfBlock(AppContext appctx)
		{
			appContext = appctx;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
		*/
		protected override void Render(HtmlBlock.Block html)
		{
			string jid = $(AMParams.JobId);
			if (jid.IsEmpty())
			{
				html.P().("Sorry, can't do anything without a JobID.").();
				return;
			}
			JobId jobID = MRApps.ToJobID(jid);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(jobID);
			if (job == null)
			{
				html.P().("Sorry, ", jid, " not found.").();
				return;
			}
			Path confPath = job.GetConfFile();
			try
			{
				ConfInfo info = new ConfInfo(job);
				html.Div().H3(confPath.ToString()).();
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
					.Table("#conf").Thead().Tr().Th(JQueryUI.Th, "key").Th(JQueryUI.Th, "value").Th(
					JQueryUI.Th, "source chain").().().Tbody();
				// Tasks table
				foreach (ConfEntryInfo entry in info.GetProperties())
				{
					StringBuilder buffer = new StringBuilder();
					string[] sources = entry.GetSource();
					//Skip the last entry, because it is always the same HDFS file, and
					// output them in reverse order so most recent is output first
					bool first = true;
					for (int i = (sources.Length - 2); i >= 0; i--)
					{
						if (!first)
						{
							// \u2B05 is an arrow <--
							buffer.Append(" \u2B05 ");
						}
						first = false;
						buffer.Append(sources[i]);
					}
					tbody.Tr().Td(entry.GetName()).Td(entry.GetValue()).Td(buffer.ToString()).();
				}
				tbody.().Tfoot().Tr().Th().Input("search_init").$type(HamletSpec.InputType.text).
					$name("key").$value("key").().().Th().Input("search_init").$type(HamletSpec.InputType
					.text).$name("value").$value("value").().().Th().Input("search_init").$type(HamletSpec.InputType
					.text).$name("source chain").$value("source chain").().().().().();
			}
			catch (IOException e)
			{
				Log.Error("Error while reading " + confPath, e);
				html.P().("Sorry got an error while reading conf file. ", confPath);
			}
		}
	}
}
