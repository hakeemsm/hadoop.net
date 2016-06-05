using System;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary><p>This class handles job end notification.</summary>
	/// <remarks>
	/// <p>This class handles job end notification. Submitters of jobs can choose to
	/// be notified of the end of a job by supplying a URL to which a connection
	/// will be established.
	/// <ul><li> The URL connection is fire and forget by default.</li> <li>
	/// User can specify number of retry attempts and a time interval at which to
	/// attempt retries</li><li>
	/// Cluster administrators can set final parameters to set maximum number of
	/// tries (0 would disable job end notification) and max time interval and a
	/// proxy if needed</li><li>
	/// The URL may contain sentinels which will be replaced by jobId and jobStatus
	/// (eg. SUCCEEDED/KILLED/FAILED) </li> </ul>
	/// </remarks>
	public class JobEndNotifier : Configurable
	{
		private const string JobId = "$jobId";

		private const string JobStatus = "$jobStatus";

		private Configuration conf;

		protected internal string userUrl;

		protected internal string proxyConf;

		protected internal int numTries;

		protected internal int waitInterval;

		protected internal int timeout;

		protected internal Uri urlToNotify;

		protected internal Proxy proxyToUse = Proxy.NoProxy;

		//Number of tries to attempt notification
		//Time (ms) to wait between retrying notification
		// Timeout (ms) on the connection and notification
		//URL to notify read from the config
		//Proxy to use for notification
		/// <summary>
		/// Parse the URL that needs to be notified of the end of the job, along
		/// with the number of retries in case of failure, the amount of time to
		/// wait between retries and proxy settings
		/// </summary>
		/// <param name="conf">the configuration</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			numTries = Math.Min(conf.GetInt(MRJobConfig.MrJobEndRetryAttempts, 0) + 1, conf.GetInt
				(MRJobConfig.MrJobEndNotificationMaxAttempts, 1));
			waitInterval = Math.Min(conf.GetInt(MRJobConfig.MrJobEndRetryInterval, 5000), conf
				.GetInt(MRJobConfig.MrJobEndNotificationMaxRetryInterval, 5000));
			waitInterval = (waitInterval < 0) ? 5000 : waitInterval;
			timeout = conf.GetInt(JobContext.MrJobEndNotificationTimeout, JobContext.DefaultMrJobEndNotificationTimeout
				);
			userUrl = conf.Get(MRJobConfig.MrJobEndNotificationUrl);
			proxyConf = conf.Get(MRJobConfig.MrJobEndNotificationProxy);
			//Configure the proxy to use if its set. It should be set like
			//proxyType@proxyHostname:port
			if (proxyConf != null && !proxyConf.Equals(string.Empty) && proxyConf.LastIndexOf
				(":") != -1)
			{
				int typeIndex = proxyConf.IndexOf("@");
				Proxy.Type proxyType = Proxy.Type.Http;
				if (typeIndex != -1 && Sharpen.Runtime.Substring(proxyConf, 0, typeIndex).CompareToIgnoreCase
					("socks") == 0)
				{
					proxyType = Proxy.Type.Socks;
				}
				string hostname = Sharpen.Runtime.Substring(proxyConf, typeIndex + 1, proxyConf.LastIndexOf
					(":"));
				string portConf = Sharpen.Runtime.Substring(proxyConf, proxyConf.LastIndexOf(":")
					 + 1);
				try
				{
					int port = System.Convert.ToInt32(portConf);
					proxyToUse = new Proxy(proxyType, new IPEndPoint(hostname, port));
					Org.Mortbay.Log.Log.Info("Job end notification using proxy type \"" + proxyType +
						 "\" hostname \"" + hostname + "\" and port \"" + port + "\"");
				}
				catch (FormatException)
				{
					Org.Mortbay.Log.Log.Warn("Job end notification couldn't parse configured proxy's port "
						 + portConf + ". Not going to use a proxy");
				}
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Notify the URL just once.</summary>
		/// <remarks>Notify the URL just once. Use best effort.</remarks>
		protected internal virtual bool NotifyURLOnce()
		{
			bool success = false;
			try
			{
				Org.Mortbay.Log.Log.Info("Job end notification trying " + urlToNotify);
				HttpURLConnection conn = (HttpURLConnection)urlToNotify.OpenConnection(proxyToUse
					);
				conn.SetConnectTimeout(timeout);
				conn.SetReadTimeout(timeout);
				conn.SetAllowUserInteraction(false);
				if (conn.GetResponseCode() != HttpURLConnection.HttpOk)
				{
					Org.Mortbay.Log.Log.Warn("Job end notification to " + urlToNotify + " failed with code: "
						 + conn.GetResponseCode() + " and message \"" + conn.GetResponseMessage() + "\""
						);
				}
				else
				{
					success = true;
					Org.Mortbay.Log.Log.Info("Job end notification to " + urlToNotify + " succeeded");
				}
			}
			catch (IOException ioe)
			{
				Org.Mortbay.Log.Log.Warn("Job end notification to " + urlToNotify + " failed", ioe
					);
			}
			return success;
		}

		/// <summary>Notify a server of the completion of a submitted job.</summary>
		/// <remarks>
		/// Notify a server of the completion of a submitted job. The user must have
		/// configured MRJobConfig.MR_JOB_END_NOTIFICATION_URL
		/// </remarks>
		/// <param name="jobReport">JobReport used to read JobId and JobStatus</param>
		/// <exception cref="System.Exception"/>
		public virtual void Notify(JobReport jobReport)
		{
			// Do we need job-end notification?
			if (userUrl == null)
			{
				Org.Mortbay.Log.Log.Info("Job end notification URL not set, skipping.");
				return;
			}
			//Do string replacements for jobId and jobStatus
			if (userUrl.Contains(JobId))
			{
				userUrl = userUrl.Replace(JobId, jobReport.GetJobId().ToString());
			}
			if (userUrl.Contains(JobStatus))
			{
				userUrl = userUrl.Replace(JobStatus, jobReport.GetJobState().ToString());
			}
			// Create the URL, ensure sanity
			try
			{
				urlToNotify = new Uri(userUrl);
			}
			catch (UriFormatException mue)
			{
				Org.Mortbay.Log.Log.Warn("Job end notification couldn't parse " + userUrl, mue);
				return;
			}
			// Send notification
			bool success = false;
			while (numTries-- > 0 && !success)
			{
				Org.Mortbay.Log.Log.Info("Job end notification attempts left " + numTries);
				success = NotifyURLOnce();
				if (!success)
				{
					Sharpen.Thread.Sleep(waitInterval);
				}
			}
			if (!success)
			{
				Org.Mortbay.Log.Log.Warn("Job end notification failed to notify : " + urlToNotify
					);
			}
			else
			{
				Org.Mortbay.Log.Log.Info("Job end notification succeeded for " + jobReport.GetJobId
					());
			}
		}
	}
}
