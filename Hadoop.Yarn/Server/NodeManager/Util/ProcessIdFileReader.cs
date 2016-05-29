using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	/// <summary>Helper functionality to read the pid from a file.</summary>
	public class ProcessIdFileReader
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ProcessIdFileReader));

		/// <summary>Get the process id from specified file path.</summary>
		/// <remarks>
		/// Get the process id from specified file path.
		/// Parses each line to find a valid number
		/// and returns the first one found.
		/// </remarks>
		/// <returns>Process Id if obtained from path specified else null</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetProcessId(Path path)
		{
			if (path == null)
			{
				throw new IOException("Trying to access process id from a null path");
			}
			Log.Debug("Accessing pid from pid file " + path);
			string processId = null;
			BufferedReader bufReader = null;
			try
			{
				FilePath file = new FilePath(path.ToString());
				if (file.Exists())
				{
					FileInputStream fis = new FileInputStream(file);
					bufReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
					while (true)
					{
						string line = bufReader.ReadLine();
						if (line == null)
						{
							break;
						}
						string temp = line.Trim();
						if (!temp.IsEmpty())
						{
							if (Shell.Windows)
							{
								// On Windows, pid is expected to be a container ID, so find first
								// line that parses successfully as a container ID.
								try
								{
									ConverterUtils.ToContainerId(temp);
									processId = temp;
									break;
								}
								catch (Exception)
								{
								}
							}
							else
							{
								// do nothing
								// Otherwise, find first line containing a numeric pid.
								try
								{
									long pid = Sharpen.Extensions.ValueOf(temp);
									if (pid > 0)
									{
										processId = temp;
										break;
									}
								}
								catch (Exception)
								{
								}
							}
						}
					}
				}
			}
			finally
			{
				// do nothing
				if (bufReader != null)
				{
					bufReader.Close();
				}
			}
			Log.Debug("Got pid " + (processId != null ? processId : "null") + " from path " +
				 path);
			return processId;
		}
	}
}
