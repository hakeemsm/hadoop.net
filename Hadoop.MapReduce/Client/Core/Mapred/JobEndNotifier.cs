using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Http.Client.Methods;
using Org.Apache.Http.Client.Params;
using Org.Apache.Http.Impl.Client;
using Org.Apache.Http.Params;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class JobEndNotifier
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JobEndNotifier).FullName
			);

		private static JobEndNotifier.JobEndStatusInfo CreateNotification(JobConf conf, JobStatus
			 status)
		{
			JobEndNotifier.JobEndStatusInfo notification = null;
			string uri = conf.GetJobEndNotificationURI();
			if (uri != null)
			{
				int retryAttempts = conf.GetInt(JobContext.MrJobEndRetryAttempts, 0);
				long retryInterval = conf.GetInt(JobContext.MrJobEndRetryInterval, 30000);
				int timeout = conf.GetInt(JobContext.MrJobEndNotificationTimeout, JobContext.DefaultMrJobEndNotificationTimeout
					);
				if (uri.Contains("$jobId"))
				{
					uri = uri.Replace("$jobId", ((JobID)status.GetJobID()).ToString());
				}
				if (uri.Contains("$jobStatus"))
				{
					string statusStr = (status.GetRunState() == JobStatus.Succeeded) ? "SUCCEEDED" : 
						(status.GetRunState() == JobStatus.Failed) ? "FAILED" : "KILLED";
					uri = uri.Replace("$jobStatus", statusStr);
				}
				notification = new JobEndNotifier.JobEndStatusInfo(uri, retryAttempts, retryInterval
					, timeout);
			}
			return notification;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private static int HttpNotification(string uri, int timeout)
		{
			DefaultHttpClient client = new DefaultHttpClient();
			client.GetParams().SetIntParameter(CoreConnectionPNames.SoTimeout, timeout).SetLongParameter
				(ClientPNames.ConnManagerTimeout, (long)timeout);
			HttpGet httpGet = new HttpGet(new URI(uri));
			httpGet.SetHeader("Accept", "*/*");
			return client.Execute(httpGet).GetStatusLine().GetStatusCode();
		}

		// for use by the LocalJobRunner, without using a thread&queue,
		// simple synchronous way
		public static void LocalRunnerNotification(JobConf conf, JobStatus status)
		{
			JobEndNotifier.JobEndStatusInfo notification = CreateNotification(conf, status);
			if (notification != null)
			{
				do
				{
					try
					{
						int code = HttpNotification(notification.GetUri(), notification.GetTimeout());
						if (code != 200)
						{
							throw new IOException("Invalid response status code: " + code);
						}
						else
						{
							break;
						}
					}
					catch (IOException ioex)
					{
						Log.Error("Notification error [" + notification.GetUri() + "]", ioex);
					}
					catch (Exception ex)
					{
						Log.Error("Notification error [" + notification.GetUri() + "]", ex);
					}
					try
					{
						Sharpen.Thread.Sleep(notification.GetRetryInterval());
					}
					catch (Exception iex)
					{
						Log.Error("Notification retry error [" + notification + "]", iex);
					}
				}
				while (notification.ConfigureForRetry());
			}
		}

		private class JobEndStatusInfo : Delayed
		{
			private string uri;

			private int retryAttempts;

			private long retryInterval;

			private long delayTime;

			private int timeout;

			internal JobEndStatusInfo(string uri, int retryAttempts, long retryInterval, int 
				timeout)
			{
				this.uri = uri;
				this.retryAttempts = retryAttempts;
				this.retryInterval = retryInterval;
				this.delayTime = Runtime.CurrentTimeMillis();
				this.timeout = timeout;
			}

			public virtual string GetUri()
			{
				return uri;
			}

			public virtual int GetRetryAttempts()
			{
				return retryAttempts;
			}

			public virtual long GetRetryInterval()
			{
				return retryInterval;
			}

			public virtual int GetTimeout()
			{
				return timeout;
			}

			public virtual bool ConfigureForRetry()
			{
				bool retry = false;
				if (GetRetryAttempts() > 0)
				{
					retry = true;
					delayTime = Runtime.CurrentTimeMillis() + retryInterval;
				}
				retryAttempts--;
				return retry;
			}

			public virtual long GetDelay(TimeUnit unit)
			{
				long n = this.delayTime - Runtime.CurrentTimeMillis();
				return unit.Convert(n, TimeUnit.Milliseconds);
			}

			public virtual int CompareTo(Delayed d)
			{
				return (int)(delayTime - ((JobEndNotifier.JobEndStatusInfo)d).delayTime);
			}

			public override bool Equals(object o)
			{
				if (!(o is JobEndNotifier.JobEndStatusInfo))
				{
					return false;
				}
				if (delayTime == ((JobEndNotifier.JobEndStatusInfo)o).delayTime)
				{
					return true;
				}
				return false;
			}

			public override int GetHashCode()
			{
				return 37 * 17 + (int)(delayTime ^ ((long)(((ulong)delayTime) >> 32)));
			}

			public override string ToString()
			{
				return "URL: " + uri + " remaining retries: " + retryAttempts + " interval: " + retryInterval;
			}
		}
	}
}
