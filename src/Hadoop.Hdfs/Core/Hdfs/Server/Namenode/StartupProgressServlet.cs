using Javax.Servlet.Http;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.IO;
using Org.Codehaus.Jackson;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Servlet that provides a JSON representation of the namenode's current startup
	/// progress.
	/// </summary>
	[System.Serializable]
	public class StartupProgressServlet : DfsServlet
	{
		private const string Count = "count";

		private const string ElapsedTime = "elapsedTime";

		private const string File = "file";

		private const string Name = "name";

		private const string Desc = "desc";

		private const string PercentComplete = "percentComplete";

		private const string Phases = "phases";

		private const string Size = "size";

		private const string Status = "status";

		private const string Steps = "steps";

		private const string Total = "total";

		public const string PathSpec = "/startupProgress";

		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			resp.SetContentType("application/json; charset=UTF-8");
			StartupProgress prog = NameNodeHttpServer.GetStartupProgressFromContext(GetServletContext
				());
			StartupProgressView view = prog.CreateView();
			JsonGenerator json = new JsonFactory().CreateJsonGenerator(resp.GetWriter());
			try
			{
				json.WriteStartObject();
				json.WriteNumberField(ElapsedTime, view.GetElapsedTime());
				json.WriteNumberField(PercentComplete, view.GetPercentComplete());
				json.WriteArrayFieldStart(Phases);
				foreach (Phase phase in view.GetPhases())
				{
					json.WriteStartObject();
					json.WriteStringField(Name, phase.GetName());
					json.WriteStringField(Desc, phase.GetDescription());
					json.WriteStringField(Status, view.GetStatus(phase).ToString());
					json.WriteNumberField(PercentComplete, view.GetPercentComplete(phase));
					json.WriteNumberField(ElapsedTime, view.GetElapsedTime(phase));
					WriteStringFieldIfNotNull(json, File, view.GetFile(phase));
					WriteNumberFieldIfDefined(json, Size, view.GetSize(phase));
					json.WriteArrayFieldStart(Steps);
					foreach (Step step in view.GetSteps(phase))
					{
						json.WriteStartObject();
						StepType type = step.GetType();
						if (type != null)
						{
							json.WriteStringField(Name, type.GetName());
							json.WriteStringField(Desc, type.GetDescription());
						}
						json.WriteNumberField(Count, view.GetCount(phase, step));
						WriteStringFieldIfNotNull(json, File, step.GetFile());
						WriteNumberFieldIfDefined(json, Size, step.GetSize());
						json.WriteNumberField(Total, view.GetTotal(phase, step));
						json.WriteNumberField(PercentComplete, view.GetPercentComplete(phase, step));
						json.WriteNumberField(ElapsedTime, view.GetElapsedTime(phase, step));
						json.WriteEndObject();
					}
					json.WriteEndArray();
					json.WriteEndObject();
				}
				json.WriteEndArray();
				json.WriteEndObject();
			}
			finally
			{
				IOUtils.Cleanup(Log, json);
			}
		}

		/// <summary>Writes a JSON number field only if the value is defined.</summary>
		/// <param name="json">JsonGenerator to receive output</param>
		/// <param name="key">String key to put</param>
		/// <param name="value">long value to put</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private static void WriteNumberFieldIfDefined(JsonGenerator json, string key, long
			 value)
		{
			if (value != long.MinValue)
			{
				json.WriteNumberField(key, value);
			}
		}

		/// <summary>Writes a JSON string field only if the value is non-null.</summary>
		/// <param name="json">JsonGenerator to receive output</param>
		/// <param name="key">String key to put</param>
		/// <param name="value">String value to put</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private static void WriteStringFieldIfNotNull(JsonGenerator json, string key, string
			 value)
		{
			if (value != null)
			{
				json.WriteStringField(key, value);
			}
		}
	}
}
